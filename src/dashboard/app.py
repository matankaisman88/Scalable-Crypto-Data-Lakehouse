import html
from datetime import date, timedelta
from pathlib import Path
from typing import List

import pandas as pd
import streamlit as st
from deltalake import DeltaTable

from src.utils.config_loader import get_paths

# ─────────────────────────────────────────────────────────────────────────────
# Formatting helpers
# ─────────────────────────────────────────────────────────────────────────────


def _format_window_label(seconds: int) -> str:
    """Format window seconds as human-readable label (e.g. 1s, 1m, 5m)."""
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        return f"{seconds // 3600}h"
    return f"{seconds // 86400}d"


def _infer_window_seconds(df: pd.DataFrame) -> int:
    """Infer aggregation window from Gold timestamp intervals."""
    if df.empty or "timestamp" not in df.columns or "symbol" not in df.columns:
        return 300
    df = df.sort_values(["symbol", "timestamp"])
    df = df.copy()
    df["_prev"] = df.groupby("symbol")["timestamp"].shift(1)
    diff = (df["timestamp"] - df["_prev"]).dropna()
    diff_sec = diff.dt.total_seconds()
    median_sec = diff_sec.median()
    if pd.isna(median_sec) or median_sec <= 0:
        return 300
    return max(1, int(round(median_sec)))


def _format_volume(val: float) -> str:
    """Format volume with K/M/B suffix (base asset volume)."""
    if val >= 1e12:
        return f"{val / 1e12:.2f}T"
    if val >= 1e9:
        return f"{val / 1e9:.2f}B"
    if val >= 1e6:
        return f"{val / 1e6:.2f}M"
    if val >= 1e3:
        return f"{val / 1e3:.2f}K"
    return f"{val:,.0f}"


def _format_price(val: float) -> str:
    """Format price with $ and commas."""
    if val >= 1000:
        return f"${val:,.2f}"
    if val >= 1:
        return f"${val:.2f}"
    return f"${val:.4f}"


def _resample_ohlcv(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    """Resample OHLCV data to a coarser timeframe."""
    if df.empty or "timestamp" not in df.columns:
        return df
    idx = df.set_index("timestamp").sort_index()
    resampled = idx.resample(rule).agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    ).dropna()
    return resampled.reset_index()


# ─────────────────────────────────────────────────────────────────────────────
# Data-loading helpers (cached)
# ─────────────────────────────────────────────────────────────────────────────


@st.cache_data(show_spinner=False)
def load_symbols_from_metadata() -> List[str]:
    """Load available symbols from coin_metadata.csv without touching the Gold table."""
    paths = get_paths()
    metadata_path = paths.get("metadata")
    if not metadata_path:
        return []
    csv_path = Path(metadata_path) / "coin_metadata.csv"
    if not csv_path.exists():
        return []
    meta = pd.read_csv(csv_path)
    if "symbol" not in meta.columns:
        return []
    return sorted(meta["symbol"].dropna().astype(str).unique().tolist())


MAX_DAYS_1S = 30  # Guardrail: prevent OOM when loading 1s data


def _date_range(start: date, end: date) -> List[str]:
    """Generate list of date strings (YYYY-MM-DD) from start to end inclusive."""
    dates = []
    d = start
    while d <= end:
        dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return dates


@st.cache_data(show_spinner=False)
def load_gold_dataframe(
    symbol: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Load Gold Delta table for symbol + date range using partition pruning."""
    paths = get_paths()
    gold_path = paths.get("gold")
    if not gold_path:
        raise RuntimeError("Gold path not found in config paths.")

    table = DeltaTable(gold_path)
    date_list = _date_range(start_date, end_date)
    df = table.to_pandas(
        partitions=[
            ("symbol", "=", symbol),
            ("date", "in", date_list),
        ]
    )

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="us")
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date

    return df


# ─────────────────────────────────────────────────────────────────────────────
# AI Query helper (cached so the Spark session survives across reruns)
# ─────────────────────────────────────────────────────────────────────────────


@st.cache_resource(show_spinner=False)
def _get_ai_helper():
    """Instantiate AIQueryHelper once per Streamlit session (heavy resource)."""
    from src.utils.ai_query_helper import AIQueryHelper

    return AIQueryHelper()


# ─────────────────────────────────────────────────────────────────────────────
# CSS
# ─────────────────────────────────────────────────────────────────────────────

_CSS = """
<style>
/* Minimize Streamlit header */
header[data-testid="stHeader"] { display: none; }
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }

/* Main content: light theme */
.main, .block-container {
    background: linear-gradient(180deg, #f8fafc 0%, #f1f5f9 100%);
    padding-top: 1.5rem;
}
h1 {
    color: #0f172a !important;
    font-weight: 700 !important;
    font-size: 1.75rem !important;
    letter-spacing: -0.02em;
}
h2, h3 {
    color: #0f172a !important;
    font-weight: 600 !important;
}

/* Dark sidebar (matches mockup) - config.toml + CSS fallback */
[data-testid="stSidebar"],
section[data-testid="stSidebar"],
div[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #1e293b 0%, #0f172a 100%) !important;
    border-right: 1px solid #334155 !important;
}
[data-testid="stSidebar"] .stMarkdown,
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3 {
    color: #e2e8f0 !important;
}
[data-testid="stSidebar"] .stCaption,
[data-testid="stSidebar"] [data-testid="stCaption"] {
    color: #94a3b8 !important;
}
[data-testid="stSidebar"] label {
    color: #e2e8f0 !important;
}
[data-testid="stSidebar"] button[kind="primary"],
[data-testid="stSidebar"] .stButton > button {
    background: linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%) !important;
    color: #ffffff !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    border: none !important;
    box-shadow: 0 2px 4px rgba(0,0,0,0.2) !important;
}
[data-testid="stSidebar"] button:hover {
    background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%) !important;
    color: #ffffff !important;
}
[data-testid="stSidebar"] [data-baseweb="select"] {
    background: #334155 !important;
    color: #f8fafc !important;
}
[data-testid="stSidebar"] .stExpander summary,
[data-testid="stSidebar"] .stExpander summary * {
    color: #0f172a !important;
    font-weight: 600 !important;
}

/* Metric cards: prominent, rounded */
.stMetric {
    background: white !important;
    padding: 1.25rem 1.5rem !important;
    border-radius: 12px !important;
    box-shadow: 0 2px 8px rgba(0,0,0,0.06) !important;
    border: 1px solid #e2e8f0 !important;
}
.stMetric label {
    color: #64748b !important;
    font-weight: 600 !important;
    font-size: 0.9rem !important;
}
.stMetric [data-testid="stMetricValue"] {
    color: #0f172a !important;
    font-weight: 700 !important;
    font-size: 1.35rem !important;
}

.stDataFrame {
    border-radius: 12px !important;
    overflow: hidden !important;
    box-shadow: 0 2px 8px rgba(0,0,0,0.06) !important;
}
.stDownloadButton button {
    background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%) !important;
    color: white !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
}
p, .stCaption, [data-testid="stCaption"], small {
    color: #1e293b !important;
}

/* AI Query: chat bubbles */
[data-testid="stChatMessage"] {
    border-radius: 12px !important;
    box-shadow: 0 2px 8px rgba(0,0,0,0.06) !important;
    margin-bottom: 1rem !important;
    padding: 1rem !important;
}
[data-testid="stChatMessage"] p {
    font-size: 1rem !important;
    line-height: 1.5 !important;
}
/* User message: dark grey bubble (Streamlit places user avatar on right) */
[data-testid="stChatMessage"] .user-msg-bubble {
    background: #334155 !important;
    color: #f8fafc !important;
    padding: 0.75rem 1rem !important;
    border-radius: 12px !important;
    display: inline-block !important;
    max-width: 85% !important;
}

/* AI Query: SQL expander - card style */
.stExpander {
    border: 1px solid #e2e8f0 !important;
    border-radius: 10px !important;
    background: white !important;
    box-shadow: 0 2px 8px rgba(0,0,0,0.06) !important;
    margin: 0.75rem 0 !important;
}
.stExpander summary {
    font-weight: 600 !important;
    color: #0f172a !important;
    padding: 0.5rem 0 !important;
}
/* Dark code block (backup for config) */
.stCodeBlock pre, .stCodeBlock code {
    background: #1e293b !important;
    color: #e2e8f0 !important;
    border-radius: 8px !important;
}

/* AI Query: chat input bar */
[data-testid="stChatInput"] {
    border-radius: 12px !important;
    box-shadow: 0 2px 8px rgba(0,0,0,0.06) !important;
}
[data-testid="stChatInput"] textarea {
    border-radius: 12px !important;
}

/* AI Query: results table */
[data-testid="stDataFrame"] thead tr th {
    background: #f1f5f9 !important;
    color: #0f172a !important;
    font-weight: 600 !important;
    border-bottom: 2px solid #e2e8f0 !important;
}

/* Tab labels */
[data-testid="stTabs"] button {
    font-size: 1.15rem !important;
    font-weight: 600 !important;
    color: #0f172a !important;
}
[data-testid="stTabs"] button[aria-selected="true"] {
    color: #2563eb !important;
}
</style>
"""

# ─────────────────────────────────────────────────────────────────────────────
# Sidebar sections
# ─────────────────────────────────────────────────────────────────────────────


def _render_refresh_button() -> None:
    """Render the 'Refresh Daily Data' button and its execution logic."""
    from src.utils.pipeline_orchestrator import run_refresh, yesterday

    st.sidebar.header("DATA MANAGEMENT")
    st.sidebar.caption(
        "Trigger the full ingestion pipeline for yesterday's data. "
        "This runs fetch → Bronze → Silver → Gold."
    )

    target = yesterday()
    if st.sidebar.button(
        f"Refresh Daily Data ({target})",
        type="primary",
        help=(
            "Runs fetch_data.sh (best-effort) then run_pipeline.sh for yesterday. "
            "Requires Docker daemon access for the fetch step."
        ),
        use_container_width=True,
    ):
        log_lines: List[str] = []
        status_placeholder = st.sidebar.empty()

        try:
            with st.spinner(f"Running pipeline for {target} …"):
                for line in run_refresh(target):
                    log_lines.append(line)
                    status_placeholder.caption(line or "…")

            status_placeholder.empty()
            st.sidebar.success(f"Pipeline finished for {target}.")
            st.cache_data.clear()
            st.sidebar.caption("Cache cleared — reload the Dashboard tab to see new data.")

        except RuntimeError as exc:
            status_placeholder.empty()
            st.sidebar.error(f"Pipeline failed: {exc}")

        if log_lines:
            with st.sidebar.expander(":blue[View pipeline log]", expanded=False):
                st.code("\n".join(log_lines), language=None)

    st.sidebar.divider()


def _render_sidebar_filters(symbols: List[str]) -> tuple:
    """Render symbol / date / price / volume filters. Returns (symbol, start, end)."""
    st.sidebar.header("FILTERS")
    st.sidebar.caption("Refine the data displayed in the chart and table.")

    symbol = st.sidebar.selectbox("Symbol", symbols, index=0)

    st.sidebar.subheader("Date range")
    st.sidebar.caption(f"Max {MAX_DAYS_1S} days (memory limit).")
    today = date.today()
    start_date = st.sidebar.date_input(
        "Start date",
        value=today - timedelta(days=7),
        min_value=date(2020, 1, 1),
        max_value=today,
    )
    end_date = st.sidebar.date_input(
        "End date",
        value=today,
        min_value=date(2020, 1, 1),
        max_value=today,
    )
    return symbol, start_date, end_date


# ─────────────────────────────────────────────────────────────────────────────
# Tab A: OHLCV Dashboard
# ─────────────────────────────────────────────────────────────────────────────


def _render_dashboard_tab(symbol: str, start_date: date, end_date: date) -> None:
    """Render the candlestick chart + data table."""
    if not symbol:
        st.info("Select a symbol to load data.")
        return

    if start_date > end_date:
        st.error("Start date must be before or equal to end date.")
        return

    span_days = (end_date - start_date).days + 1
    if span_days > MAX_DAYS_1S:
        st.error(
            f"Date range is {span_days} days. Maximum {MAX_DAYS_1S} days allowed "
            "to stay within the memory limit. Please select a shorter range."
        )
        return

    with st.spinner(f"Loading Gold data for {symbol} ({start_date} to {end_date})…"):
        try:
            df = load_gold_dataframe(symbol, start_date, end_date)
        except Exception as exc:
            msg = str(exc)
            if "no log files" in msg or "TableNotFoundError" in msg or "not a Delta table" in msg.lower():
                st.info(
                    "The Gold Delta table doesn't exist yet. "
                    "Use **Refresh Daily Data** in the sidebar to run the ingestion pipeline first.",
                    icon="ℹ️",
                )
            else:
                st.error(f"Failed to load Gold data: {exc}")
            return

    if df.empty:
        st.warning(
            f"No Gold data for {symbol} in {start_date}–{end_date}. "
            "Run the pipeline first (or use the Refresh button in the sidebar)."
        )
        return

    st.title("Crypto Analytics Dashboard")

    required_cols = {"symbol", "timestamp", "open", "high", "low", "close", "volume"}
    missing_cols = required_cols.difference(df.columns)
    if missing_cols:
        st.error(f"Missing expected columns in Gold table: {', '.join(sorted(missing_cols))}")
        st.dataframe(df.head())
        return

    filtered = df

    # ── Timeframe (chart aggregation) ────────────────────────────────────
    st.sidebar.subheader("Timeframe")
    timeframe = st.sidebar.radio(
        "Chart aggregation",
        ["1m", "1H", "4H", "1D", "1W"],
        horizontal=True,
        format_func=lambda x: x,
    )
    resample_rules = {"1m": None, "1H": "1h", "4H": "4h", "1D": "1D", "1W": "1W"}
    resample_rule = resample_rules[timeframe]

    # ── Price & volume filters ────────────────────────────────────────────
    st.sidebar.subheader("Price & Volume")
    price_min = float(filtered["close"].min())
    price_max = float(filtered["close"].max())
    price_range = st.sidebar.slider(
        "Close price range",
        min_value=price_min,
        max_value=price_max,
        value=(price_min, price_max),
        format="%.2f",
    )
    filtered = filtered[
        (filtered["close"] >= price_range[0]) & (filtered["close"] <= price_range[1])
    ]

    vol_min = float(filtered["volume"].min())
    vol_max = float(filtered["volume"].max())
    vol_threshold = st.sidebar.slider(
        "Min volume",
        min_value=vol_min,
        max_value=vol_max,
        value=vol_min,
        format="%.0f",
    )
    filtered = filtered[filtered["volume"] >= vol_threshold]

    sort_order = st.sidebar.radio("Table sort", ["Newest first", "Oldest first"], horizontal=True)
    filtered = filtered.sort_values("timestamp", ascending=True)

    if filtered.empty:
        st.warning("No data for the selected filters.")
        return

    MAX_CHART_ROWS = 5000
    MAX_TABLE_ROWS = 1000
    chart_df = filtered.iloc[-MAX_CHART_ROWS:] if len(filtered) > MAX_CHART_ROWS else filtered.copy()
    if resample_rule:
        chart_df = _resample_ohlcv(chart_df, resample_rule)
    if chart_df.empty:
        st.warning("No chart data after resampling. Try a different timeframe or date range.")
        return
    table_df = filtered.tail(MAX_TABLE_ROWS)
    if sort_order == "Newest first":
        table_df = table_df.iloc[::-1]

    latest = filtered.iloc[-1]
    first = filtered.iloc[0]
    total_volume = float(filtered["volume"].sum())
    close_price = float(latest["close"])
    price_change = float(latest["close"] - first["close"])
    pct_change = (price_change / float(first["close"])) * 100 if first["close"] else 0.0

    col1, col2, col3 = st.columns(3)
    col1.metric("Symbol Price", _format_price(close_price))
    col2.metric("Total Volume", _format_volume(total_volume))
    change_val = f"+{_format_price(price_change)}" if price_change >= 0 else _format_price(price_change)
    col3.metric("24h Price Change", change_val, delta=f"{pct_change:+.2f}%")

    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    # Candlestick + volume bars (mockup style)
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.7, 0.3],
    )
    fig.add_trace(
        go.Candlestick(
            x=chart_df["timestamp"],
            open=chart_df["open"],
            high=chart_df["high"],
            low=chart_df["low"],
            close=chart_df["close"],
            increasing_line_color="#059669",
            decreasing_line_color="#dc2626",
            name="Price",
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Bar(
            x=chart_df["timestamp"],
            y=chart_df["volume"],
            marker_color="#3b82f6",
            name="Volume",
            showlegend=False,
        ),
        row=2,
        col=1,
    )
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.95)",
        margin=dict(l=50, r=80, t=80, b=40),
        height=550,
        font=dict(color="#0f172a", size=12),
        title=dict(
            text=f"{symbol} | OHLCV Candlestick Chart ({timeframe})",
            font=dict(size=18, color="#0f172a"),
            x=0.5,
            xanchor="center",
        ),
        xaxis_rangeslider_visible=False,
        xaxis2_title="Time",
        showlegend=False,
        annotations=[
            dict(
                x=chart_df["timestamp"].iloc[-1],
                y=close_price,
                xref="x",
                yref="y",
                xanchor="left",
                text=f" {_format_price(close_price)} →",
                showarrow=False,
                font=dict(size=12, color="#0f172a"),
                bgcolor="rgba(255,255,255,0.9)",
                borderpad=4,
            )
        ],
    )
    fig.update_xaxes(
        gridcolor="#cbd5e1",
        tickfont=dict(color="#0f172a", size=11),
        row=1,
        col=1,
    )
    fig.update_xaxes(
        gridcolor="#cbd5e1",
        tickfont=dict(color="#0f172a", size=11),
        row=2,
        col=1,
    )
    fig.update_yaxes(
        title_text="Price",
        gridcolor="#cbd5e1",
        tickfont=dict(color="#0f172a", size=11),
        row=1,
        col=1,
    )
    fig.update_yaxes(
        title_text="Volume",
        gridcolor="#cbd5e1",
        tickfont=dict(color="#0f172a", size=11),
        row=2,
        col=1,
    )

    if len(filtered) > MAX_CHART_ROWS:
        st.caption(f"Showing last {MAX_CHART_ROWS:,} of {len(filtered):,} rows.")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Data")
    if len(filtered) > MAX_TABLE_ROWS:
        st.caption(f"Showing last {MAX_TABLE_ROWS:,} of {len(filtered):,} rows.")
    cols = [
        "timestamp",
        *(["date"] if "date" in table_df.columns else []),
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    st.dataframe(table_df[cols], use_container_width=True)

    download_df = filtered[cols].copy()
    csv = download_df.to_csv(index=False)
    date_suffix = f"_{start_date}_{end_date}" if start_date and end_date else ""
    filename = f"gold_ohlcv_{symbol}{date_suffix}.csv".replace(" ", "_")
    st.download_button(
        label=f"Download as CSV ({len(filtered):,} rows)",
        data=csv,
        file_name=filename,
        mime="text/csv",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Tab B: AI Query chat
# ─────────────────────────────────────────────────────────────────────────────


def _render_ai_chat_tab() -> None:
    """Render the NL-to-SQL chat interface."""
    st.markdown(
        '<h1 style="text-align: center; font-size: 2rem; font-weight: 700; color: #0f172a; margin-bottom: 0.25rem;">Crypto Data AI Query</h1>'
        '<p style="text-align: center; color: #64748b; font-size: 0.95rem; margin-bottom: 1.5rem;">'
        "Ask questions about the Silver or Gold Delta tables in plain English. "
        "The assistant translates your question into Spark SQL, runs it, and explains the results.</p>",
        unsafe_allow_html=True,
    )

    # ── API key check ─────────────────────────────────────────────────────
    import os

    if not os.getenv("OPENAI_API_KEY"):
        st.warning(
            "**OPENAI_API_KEY** is not set. "
            "Add it to your `.env` file at the project root:\n\n"
            "```\nOPENAI_API_KEY=sk-...\n```\n\n"
            "Then restart the dashboard.",
            icon="🔑",
        )
        return

    # ── Example prompts ───────────────────────────────────────────────────
    with st.expander("Example questions", expanded=False):
        st.markdown("**Gold** — Clean, analytics-ready (OHLCV, volume, trades). Use for most questions.")
        st.markdown("- *What was the highest and lowest price for BTCUSDT on 2024-01-15?*")
        st.markdown("- *What is the total trading volume for ETHUSDT on 2024-01-10?*")
        st.markdown("- *Show the 10 most recent candles (open, high, low, close) for BNBUSDT on 2024-01-18.*")
        st.markdown("- *Which hour had the highest average close price for BTCUSDT on 2024-01-15?*")
        st.markdown("")
        st.markdown("**Silver** — Same data + extra columns (quote_asset_volume, taker_buy_base/quote, coin_name). Use only when you need these.")
        st.markdown("- *What was the total quote asset volume for BTCUSDT on 2024-01-15?*")
        st.markdown("- *Show taker buy base and taker buy quote for ETHUSDT on 2024-01-20.*")
        st.markdown("- *List symbols with their coin names from the silver table on 2024-01-18.*")

    # ── Chat session state ────────────────────────────────────────────────
    if "ai_chat_messages" not in st.session_state:
        st.session_state.ai_chat_messages = []

    # Render existing conversation
    for msg in st.session_state.ai_chat_messages:
        with st.chat_message(msg["role"]):
            if msg["role"] == "user":
                escaped = html.escape(msg["content"])
                st.markdown(f'<span class="user-msg-bubble">{escaped}</span>', unsafe_allow_html=True)
            else:
                # Assistant messages carry extra structured data
                if msg.get("error"):
                    st.error(msg["error"])
                else:
                    intro = "Of course! I've generated the SQL query and retrieved the results for you:" if msg.get("sql") and msg.get("dataframe") is not None and not msg["dataframe"].empty else ""
                    if intro:
                        st.markdown(intro)

                if msg.get("sql"):
                    with st.expander("Generated SQL", expanded=False):
                        st.code(msg["sql"], language="sql")

                if msg.get("dataframe") is not None and not msg["dataframe"].empty:
                    st.dataframe(msg["dataframe"], use_container_width=True)
                    st.caption(f"{len(msg['dataframe']):,} row(s) returned.")

                if not msg.get("error") and msg.get("explanation"):
                    st.markdown(msg["explanation"])

    # ── Clear history button ──────────────────────────────────────────────
    if st.session_state.ai_chat_messages:
        if st.button("Clear conversation", key="clear_ai_chat"):
            st.session_state.ai_chat_messages = []
            st.rerun()

    # ── Input box ────────────────────────────────────────────────────────
    user_input = st.chat_input(
        "Ask anything about the crypto data…  (e.g. 'Show BTCUSDT closes on 2024-01-15')"
    )

    if not user_input:
        return

    # Store and display user message immediately
    st.session_state.ai_chat_messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        escaped = html.escape(user_input)
        st.markdown(f'<span class="user-msg-bubble">{escaped}</span>', unsafe_allow_html=True)

    # ── Call the AI helper ────────────────────────────────────────────────
    with st.chat_message("assistant"):
        with st.spinner("Thinking…"):
            try:
                helper = _get_ai_helper()
                result = helper.query(user_input)
            except ValueError as exc:
                # API key missing or invalid at instantiation time
                result = {
                    "sql": "",
                    "explanation": "",
                    "dataframe": pd.DataFrame(),
                    "error": str(exc),
                }

        if result["error"]:
            st.error(result["error"])
        else:
            intro = "Of course! I've generated the SQL query and retrieved the results for you:" if result.get("sql") and result.get("dataframe") is not None and not result["dataframe"].empty else ""
            if intro:
                st.markdown(intro)

        if result.get("sql"):
            with st.expander("Generated SQL", expanded=True):
                st.code(result["sql"], language="sql")

        df = result.get("dataframe")
        if df is not None and not df.empty:
            st.dataframe(df, use_container_width=True)
            st.caption(f"{len(df):,} row(s) returned.")

        if not result.get("error") and result.get("explanation"):
            st.markdown(result["explanation"])

    # Persist assistant turn to session state
    st.session_state.ai_chat_messages.append(
        {
            "role": "assistant",
            "content": result.get("explanation", ""),
            "sql": result.get("sql", ""),
            "dataframe": result.get("dataframe"),
            "error": result.get("error"),
        }
    )


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────


def main() -> None:
    st.set_page_config(
        page_title="Crypto Analytics Dashboard",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown(_CSS, unsafe_allow_html=True)

    # ── Sidebar: refresh button (top) then filters ────────────────────────
    _render_refresh_button()

    symbols = load_symbols_from_metadata()
    if not symbols:
        st.warning(
            "No symbols found in data/metadata/coin_metadata.csv. "
            "Add symbols there or run the Bronze ingestion first."
        )
        return

    symbol, start_date, end_date = _render_sidebar_filters(symbols)

    # ── Main area: two tabs ───────────────────────────────────────────────
    tab_dashboard, tab_ai = st.tabs(["📊 Dashboard", "🤖 AI Query"])

    with tab_dashboard:
        _render_dashboard_tab(symbol, start_date, end_date)

    with tab_ai:
        _render_ai_chat_tab()


if __name__ == "__main__":
    main()
