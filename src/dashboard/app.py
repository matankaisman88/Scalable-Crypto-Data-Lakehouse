import pandas as pd
import streamlit as st
from deltalake import DeltaTable

from src.utils.config_loader import get_paths


@st.cache_data(show_spinner=False)
def load_gold_dataframe() -> pd.DataFrame:
    paths = get_paths()
    gold_path = paths.get("gold")
    if not gold_path:
        raise RuntimeError("Gold path not found in config paths.")

    table = DeltaTable(gold_path)
    df = table.to_pandas()

    if "timestamp" in df.columns:
        # Gold timestamp is stored as Unix epoch in milliseconds
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
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
        .main {
            background-color: #0e1117;
            color: #e0e3e7;
        }
        .stMetric {
            background-color: #161925;
            padding: 0.75rem 1rem;
            border-radius: 0.5rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.title("Gold Layer - 5m OHLCV")
    st.caption("Interactive visualization of the Gold Delta table (5-minute OHLCV).")

    with st.spinner("Loading Gold Delta table..."):
        df = load_gold_dataframe()

    if df.empty:
        st.warning("Gold table is empty. Run the Gold job first.")
        return

    required_cols = {"symbol", "timestamp", "open", "high", "low", "close", "volume"}
    missing_cols = required_cols.difference(df.columns)
    if missing_cols:
        st.error(f"Missing expected columns in Gold table: {', '.join(sorted(missing_cols))}")
        st.dataframe(df.head())
        return

    st.sidebar.header("Filters")

    symbols = sorted(df["symbol"].dropna().unique().tolist())
    default_symbol = symbols[0] if symbols else None
    symbol = st.sidebar.selectbox("Symbol", symbols, index=0 if default_symbol else None)

    symbol_df = df[df["symbol"] == symbol] if symbol else df

    if "date" in symbol_df.columns:
        dates = sorted(symbol_df["date"].dropna().unique().tolist())
        if dates:
            start_date, end_date = st.sidebar.select_slider(
                "Date range",
                options=dates,
                value=(dates[0], dates[-1]),
            )
            mask = (symbol_df["date"] >= start_date) & (symbol_df["date"] <= end_date)
            filtered = symbol_df[mask]
        else:
            filtered = symbol_df
    else:
        filtered = symbol_df

    filtered = filtered.sort_values("timestamp")

    if filtered.empty:
        st.warning("No data for the selected filters.")
        return

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
                x=filtered["timestamp"],
                open=filtered["open"],
                high=filtered["high"],
                low=filtered["low"],
                close=filtered["close"],
                increasing_line_color="#26a69a",
                decreasing_line_color="#ef5350",
                name="Price",
            )
        ]
    )
    fig.update_layout(
        template="plotly_dark",
        margin=dict(l=0, r=0, t=40, b=0),
        xaxis_title="Time",
        yaxis_title="Price",
        height=600,
    )

    st.subheader("Candlestick Chart")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Data")
    st.dataframe(
        filtered[
            [
                "timestamp",
                *(["date"] if "date" in filtered.columns else []),
                "open",
                "high",
                "low",
                "close",
                "volume",
            ]
        ],
        use_container_width=True,
    )


if __name__ == "__main__":
    main()

