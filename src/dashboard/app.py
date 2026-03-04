from datetime import timedelta

import pandas as pd
import streamlit as st
from deltalake import DeltaTable

from src.utils.config_loader import get_paths


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
        return 300  # fallback
    df = df.sort_values(["symbol", "timestamp"])
    df = df.copy()
    df["_prev"] = df.groupby("symbol")["timestamp"].shift(1)
    diff = (df["timestamp"] - df["_prev"]).dropna()
    diff_sec = diff.dt.total_seconds()
    median_sec = diff_sec.median()
    if pd.isna(median_sec) or median_sec <= 0:
        return 300
    return max(1, int(round(median_sec)))


@st.cache_data(show_spinner=False)
def load_gold_dataframe() -> pd.DataFrame:
    paths = get_paths()
    gold_path = paths.get("gold")
    if not gold_path:
        raise RuntimeError("Gold path not found in config paths.")

    table = DeltaTable(gold_path)
    df = table.to_pandas()

    if "timestamp" in df.columns:
        # Gold timestamp is stored as Unix epoch in microseconds
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="us")
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date

    return df


def main() -> None:
    st.set_page_config(
        page_title="Crypto OHLCV Dashboard",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.markdown(
        """
        <style>
        /* Light, modern theme */
        .main, .block-container {
            background: linear-gradient(180deg, #f8fafc 0%, #f1f5f9 100%);
            padding-top: 1.5rem;
        }
        h1 {
            color: #0f172a !important;
            font-weight: 700 !important;
            letter-spacing: -0.02em;
        }
        .stMetric {
            background: white;
            padding: 1rem 1.25rem;
            border-radius: 12px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.08);
            border: 1px solid #e2e8f0;
        }
        .stMetric label {
            color: #64748b !important;
            font-weight: 500 !important;
        }
        .stMetric [data-testid="stMetricValue"] {
            color: #0f172a !important;
            font-weight: 600 !important;
        }
        div[data-testid="stSidebar"] {
            background: white;
            border-right: 1px solid #e2e8f0;
        }
        div[data-testid="stSidebar"] .stMarkdown {
            color: #334155;
        }
        .stDataFrame {
            border-radius: 12px;
            overflow: hidden;
            box-shadow: 0 1px 3px rgba(0,0,0,0.08);
        }
        .stDownloadButton button {
            background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%) !important;
            color: white !important;
            border-radius: 8px !important;
            font-weight: 500 !important;
        }
        /* Improve text contrast */
        p, .stCaption, [data-testid="stCaption"], small {
            color: #1e293b !important;
        }
        h2, h3 {
            color: #0f172a !important;
            font-weight: 600 !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    with st.spinner("Loading Gold Delta table..."):
        df = load_gold_dataframe()

    if df.empty:
        st.warning("Gold table is empty. Run the Gold job first.")
        return

    window_sec = _infer_window_seconds(df)
    window_label = _format_window_label(window_sec)
    st.title(f"Gold Layer - {window_label} OHLCV")
    st.caption(f"Interactive visualization of the Gold Delta table ({window_label} OHLCV).")

    required_cols = {"symbol", "timestamp", "open", "high", "low", "close", "volume"}
    missing_cols = required_cols.difference(df.columns)
    if missing_cols:
        st.error(f"Missing expected columns in Gold table: {', '.join(sorted(missing_cols))}")
        st.dataframe(df.head())
        return

    st.sidebar.header("Filters")
    st.sidebar.caption("Refine the data displayed in the chart and table.")

    symbols = sorted(df["symbol"].dropna().unique().tolist())
    default_symbol = symbols[0] if symbols else None
    symbol = st.sidebar.selectbox("Symbol", symbols, index=0 if default_symbol else None)

    symbol_df = df[df["symbol"] == symbol] if symbol else df
    start_date = end_date = None

    if "date" in symbol_df.columns:
        dates = sorted(symbol_df["date"].dropna().unique().tolist())
        if dates:
            # Default to last 30 days to avoid MessageSizeError
            cutoff = max(dates) - timedelta(days=30) if dates else None
            default_start = next((d for d in dates if d >= cutoff), dates[0])
            default_end = dates[-1]
            start_date, end_date = st.sidebar.select_slider(
                "Date range",
                options=dates,
                value=(default_start, default_end),
            )
            mask = (symbol_df["date"] >= start_date) & (symbol_df["date"] <= end_date)
            filtered = symbol_df[mask]
        else:
            filtered = symbol_df
    else:
        filtered = symbol_df

    # Price range filter
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
    filtered = filtered.sort_values("timestamp", ascending=True)  # chronological for chart

    if filtered.empty:
        st.warning("No data for the selected filters.")
        return

    # Limit data sent to browser to avoid MessageSizeError
    MAX_CHART_ROWS = 5000
    MAX_TABLE_ROWS = 1000
    chart_df = filtered.iloc[-MAX_CHART_ROWS:] if len(filtered) > MAX_CHART_ROWS else filtered
    table_df = filtered.tail(MAX_TABLE_ROWS)
    if sort_order == "Newest first":
        table_df = table_df.iloc[::-1]

    latest = filtered.iloc[-1]
    first = filtered.iloc[0]
    total_volume = float(filtered["volume"].sum())
    price_change = float(latest["close"] - first["close"])
    pct_change = (price_change / float(first["close"])) * 100 if first["close"] else 0.0

    col1, col2, col3 = st.columns(3)
    col1.metric("Symbol", symbol)
    col2.metric("Total Volume", f"{total_volume:,.0f}")
    col3.metric(
        "Price Change",
        f"{latest['close']:.4f}",
        f"{price_change:+.4f} ({pct_change:+.2f}%)",
    )

    import plotly.graph_objects as go

    fig = go.Figure(
        data=[
            go.Candlestick(
                x=chart_df["timestamp"],
                open=chart_df["open"],
                high=chart_df["high"],
                low=chart_df["low"],
                close=chart_df["close"],
                increasing_line_color="#059669",
                decreasing_line_color="#dc2626",
                name="Price",
            )
        ]
    )
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.95)",
        margin=dict(l=0, r=0, t=40, b=0),
        xaxis_title="Time",
        yaxis_title="Price",
        height=600,
        font=dict(color="#0f172a", size=12),
        title_font=dict(color="#0f172a", size=14),
        xaxis=dict(
            gridcolor="#cbd5e1",
            tickfont=dict(color="#0f172a", size=11),
            title_font=dict(color="#0f172a", size=12),
        ),
        yaxis=dict(
            gridcolor="#cbd5e1",
            tickfont=dict(color="#0f172a", size=11),
            title_font=dict(color="#0f172a", size=12),
        ),
    )

    st.subheader("Candlestick Chart")
    if len(filtered) > MAX_CHART_ROWS:
        st.caption(f"Showing last {MAX_CHART_ROWS:,} of {len(filtered):,} rows.")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Data")
    if len(filtered) > MAX_TABLE_ROWS:
        st.caption(f"Showing last {MAX_TABLE_ROWS:,} of {len(filtered):,} rows.")
    cols = ["timestamp", *(["date"] if "date" in table_df.columns else []), "open", "high", "low", "close", "volume"]
    st.dataframe(table_df[cols], use_container_width=True)

    # Download filtered data as CSV (all rows matching filters, not just displayed subset)
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


if __name__ == "__main__":
    main()

