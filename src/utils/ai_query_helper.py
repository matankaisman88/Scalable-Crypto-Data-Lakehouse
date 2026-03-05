"""
Natural Language to Spark SQL query helper.

Translates user questions into Spark SQL, executes them against the Delta
Lakehouse via spark.sql(), and returns the generated SQL, results, and an
explanation — all in one round trip.
"""

import json
import os
import re
from typing import Any, Dict, Optional

import pandas as pd

# ─────────────────────────────────────────────────────────────
# Table DDLs injected into the system prompt as authoritative
# schema reference.  Kept here (not imported from schemas.py)
# so the LLM always sees exact SQL DDL, not Python StructType.
# ─────────────────────────────────────────────────────────────

_SILVER_DDL = """\
CREATE TABLE silver_klines (
    open_time           BIGINT    NOT NULL  -- Unix epoch in MICROSECONDS
   ,open                DOUBLE    NOT NULL
   ,high                DOUBLE    NOT NULL
   ,low                 DOUBLE    NOT NULL
   ,close               DOUBLE    NOT NULL
   ,volume              DOUBLE    NOT NULL
   ,close_time          BIGINT    NOT NULL  -- Unix epoch in MICROSECONDS
   ,quote_asset_volume  DOUBLE    NOT NULL
   ,num_trades          BIGINT    NOT NULL
   ,taker_buy_base      DOUBLE    NOT NULL
   ,taker_buy_quote     DOUBLE    NOT NULL
   ,ignore              BIGINT    NOT NULL
   ,symbol              STRING    NOT NULL
   ,date                DATE      NOT NULL   -- partition key
   ,ingestion_date      DATE      NOT NULL
   ,ingestion_ts        TIMESTAMP NOT NULL
   ,coin_name           STRING
) USING DELTA
PARTITIONED BY (symbol, date);"""

_GOLD_DDL = """\
CREATE TABLE gold_ohlcv (
    symbol      STRING  NOT NULL
   ,timestamp   BIGINT  NOT NULL  -- Unix epoch in MICROSECONDS
   ,open        DOUBLE  NOT NULL
   ,high        DOUBLE  NOT NULL
   ,low         DOUBLE  NOT NULL
   ,close       DOUBLE  NOT NULL
   ,volume      DOUBLE  NOT NULL
   ,num_trades  BIGINT  NOT NULL
   ,date        DATE    NOT NULL   -- partition key
) USING DELTA
PARTITIONED BY (symbol, date);"""

_SYSTEM_PROMPT = f"""You are a Spark SQL expert embedded in a cryptocurrency OHLCV data lakehouse.

## Available Tables

### silver_klines — 1-second raw klines from Binance
{_SILVER_DDL}

### gold_ohlcv — 5-minute aggregated OHLCV (Gold layer)
{_GOLD_DDL}

---

## ⚠️  CRITICAL: Timestamps Are Stored in MICROSECONDS

Both `open_time` (silver_klines) and `timestamp` (gold_ohlcv) are BIGINT
columns that hold Unix epoch **microseconds** — NOT milliseconds or seconds.

To convert to a human-readable datetime you MUST divide by 1 000 000:
    FROM_UNIXTIME(timestamp / 1000000)           → STRING  'YYYY-MM-DD HH:mm:ss'
    CAST(FROM_UNIXTIME(timestamp / 1000000) AS TIMESTAMP)  → TIMESTAMP

✅ Correct:  SELECT FROM_UNIXTIME(timestamp/1000000) AS ts, close FROM gold_ohlcv ...
❌ Wrong:    SELECT FROM_UNIXTIME(timestamp) AS ts ...   -- off by 1 000 000×

---

## Resource Constraints (1 GB executor RAM)

1. **ALWAYS** end every query with `LIMIT 100` (hard ceiling — reduce if aggregating).
2. **ALWAYS** filter by `symbol` AND `date` to exploit partition pruning and avoid
   full table scans:
       WHERE symbol = 'BTCUSDT' AND date = '2024-01-15'
3. Avoid unbounded aggregations, cross-joins, and cartesian products.

---

## Safety Rules

NEVER generate DROP, DELETE, UPDATE, INSERT, CREATE, ALTER, TRUNCATE, or MERGE.
Only pure SELECT queries are allowed.

---

## Response Format

Reply with ONLY a valid JSON object — no markdown fences, no extra keys:
{{
  "sql":         "<complete Spark SQL SELECT query>",
  "explanation": "<1–2 sentences: what the query does and what its results reveal>"
}}"""

# Patterns that must never appear in generated SQL
_FORBIDDEN_PATTERN = re.compile(
    r"\b(DROP|DELETE|UPDATE|INSERT|CREATE|ALTER|TRUNCATE|MERGE)\b",
    re.IGNORECASE,
)

_LIMIT_PATTERN = re.compile(r"\bLIMIT\s+(\d+)", re.IGNORECASE)

_QUERY_HARD_CAP = 100


# ─────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────


def _enforce_limit(sql: str, cap: int = _QUERY_HARD_CAP) -> str:
    """Ensure a LIMIT clause is present and does not exceed *cap*."""
    stripped = sql.strip().rstrip(";")
    match = _LIMIT_PATTERN.search(stripped)
    if match:
        if int(match.group(1)) > cap:
            stripped = _LIMIT_PATTERN.sub(f"LIMIT {cap}", stripped)
    else:
        stripped += f"\nLIMIT {cap}"
    return stripped


def _validate_sql(sql: str) -> None:
    """Raise ValueError if the query contains forbidden DML/DDL keywords."""
    if _FORBIDDEN_PATTERN.search(sql):
        raise ValueError(
            "Generated SQL contains a forbidden statement (DROP / DELETE / UPDATE / etc.). "
            "Only SELECT queries are permitted."
        )


def _register_delta_views(spark) -> None:  # type: ignore[no-untyped-def]
    """
    Register both Delta tables as Spark temp views.

    Called before every sql() execution so views are always up to date with
    the latest Delta snapshots, even across Streamlit reruns.
    """
    from src.utils.config_loader import get_paths  # local import avoids circular dep

    paths = get_paths()

    silver_path = paths.get("silver")
    if silver_path:
        (
            spark.read.format("delta")
            .load(silver_path)
            .createOrReplaceTempView("silver_klines")
        )

    gold_path = paths.get("gold")
    if gold_path:
        (
            spark.read.format("delta")
            .load(gold_path)
            .createOrReplaceTempView("gold_ohlcv")
        )


# ─────────────────────────────────────────────────────────────
# Public interface
# ─────────────────────────────────────────────────────────────


class AIQueryHelper:
    """
    Translate a natural-language question to Spark SQL and execute it.

    Usage::

        helper = AIQueryHelper()          # reads OPENAI_API_KEY from env
        result = helper.query("What was the highest BTC close price on 2024-01-15?")
        print(result["sql"])
        print(result["dataframe"])

    The returned dict always has these keys:
        sql         (str)          – generated Spark SQL (empty on LLM failure)
        explanation (str)          – human-readable summary
        dataframe   (pd.DataFrame) – query results (empty on error)
        error       (str | None)   – error message or None on success
    """

    def __init__(self, api_key: Optional[str] = None) -> None:
        from openai import OpenAI  # lazy import: only required at instantiation

        key = api_key or os.getenv("OPENAI_API_KEY")
        if not key:
            raise ValueError(
                "OPENAI_API_KEY is not set. Add it to your .env file or export it "
                "in the shell before starting the dashboard."
            )
        self._client = OpenAI(api_key=key)
        self._model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    def query(self, question: str) -> Dict[str, Any]:
        """
        Full round-trip: NL question → LLM → SQL → Spark execution → result.

        Never raises; errors are captured in the returned ``error`` field.
        """
        sql = ""
        explanation = ""

        try:
            # ── 1. Ask the LLM ──────────────────────────────────────────
            response = self._client.chat.completions.create(
                model=self._model,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": question},
                ],
                temperature=0,
                response_format={"type": "json_object"},
            )
            raw_content = response.choices[0].message.content
            parsed = json.loads(raw_content)

            sql = parsed.get("sql", "").strip()
            explanation = parsed.get("explanation", "")

            if not sql.upper().lstrip().startswith("SELECT"):
                raise ValueError(
                    f"LLM returned a non-SELECT statement. Raw response: {raw_content!r}"
                )

            # ── 2. Safety + normalise ────────────────────────────────────
            _validate_sql(sql)
            sql = _enforce_limit(sql)

            # ── 3. Execute via Spark ─────────────────────────────────────
            from src.utils.spark_session import get_spark_session  # lazy import

            spark = get_spark_session(app_name="CryptoLakehouse-AIQuery")
            _register_delta_views(spark)
            result_pdf = spark.sql(sql).toPandas()

            return {
                "sql": sql,
                "explanation": explanation,
                "dataframe": result_pdf,
                "error": None,
            }

        except Exception as exc:  # noqa: BLE001
            return {
                "sql": sql,
                "explanation": explanation,
                "dataframe": pd.DataFrame(),
                "error": str(exc),
            }
