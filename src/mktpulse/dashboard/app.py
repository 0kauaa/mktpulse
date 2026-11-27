"""dashboard streamlit do marketpulse."""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

try:
    from data import (
        filter_period,
        lake_paths,
        load_gold,
        load_metadata,
        load_silver,
        merge_anomalies,
        summarize_symbols,
        top_anomalies,
    )
except ImportError:
    from dashboard.data import (
        filter_period,
        lake_paths,
        load_gold,
        load_metadata,
        load_silver,
        merge_anomalies,
        summarize_symbols,
        top_anomalies,
    )


lake_root = Path(os.getenv("MKT_LAKE_ROOT", "data/lake"))
source = os.getenv("MKT_SOURCE", "yahoo")
silver_path, gold_path, metadata_path = lake_paths(lake_root, source)

st.set_page_config(
    page_title="MarketPulse",
    layout="wide",
)


def _mtime(path: Path) -> float:
    return path.stat().st_mtime if path.exists() else 0.0


@st.cache_data(show_spinner=False)
def _silver_cached(path: str, modified: float) -> pd.DataFrame:
    return load_silver(Path(path))


@st.cache_data(show_spinner=False)
def _gold_cached(path: str, modified: float) -> pd.DataFrame:
    return load_gold(Path(path))


@st.cache_data(show_spinner=False)
def _metadata_cached(path: str, modified: float) -> dict:
    return load_metadata(Path(path))


st.title("MarketPulse")
st.caption("OHLCV da B3, indicadores, qualidade e anomalias em um unico lugar.")

if not silver_path.exists() or next(silver_path.rglob("*.parquet"), None) is None:
    st.error("Camada silver nao encontrada. Rode `uv run mktpulse transform --full-refresh`.")
    st.stop()

silver = _silver_cached(str(silver_path), _mtime(silver_path))
gold = _gold_cached(str(gold_path), _mtime(gold_path))
metadata = _metadata_cached(str(metadata_path), _mtime(metadata_path))
merged = merge_anomalies(silver, gold)
filtered = merged

symbols = sorted(merged["symbol"].dropna().unique().tolist())
if not symbols:
    st.error("Nenhum simbolo encontrado no silver.")
    st.stop()

anomaly_counts = merged.loc[merged["is_anomaly"], "symbol"].value_counts()
if not anomaly_counts.empty:
    default_symbol = str(anomaly_counts.index[0])
elif "PETR4" in symbols:
    default_symbol = "PETR4"
else:
    default_symbol = symbols[0]
selected = st.sidebar.selectbox(
    "Ativo",
    symbols,
    index=symbols.index(default_symbol),
)

minimum = merged["trade_date"].min().date()
maximum = merged["trade_date"].max().date()
default_start = max(minimum, maximum - timedelta(days=365))
period = st.sidebar.date_input(
    "Periodo",
    value=(default_start, maximum),
    min_value=minimum,
    max_value=maximum,
)

if isinstance(period, tuple) and len(period) == 2:
    start_date, end_date = period
elif isinstance(period, date):
    start_date, end_date = minimum, period
else:
    start_date, end_date = minimum, maximum

filtered = filter_period(merged, start_date, end_date)
detail = filtered[filtered["symbol"] == selected].sort_values("trade_date")
all_anomalies = int(filtered["is_anomaly"].sum())

if gold.empty:
    st.warning("Anomalias ainda nao calculadas. Rode `uv run mktpulse detect` para gerar o gold.")

st.sidebar.markdown("---")
st.sidebar.metric("Linhas silver", f"{len(silver):,}".replace(",", "."))
st.sidebar.metric("Anomalias no periodo", all_anomalies)
if metadata:
    st.sidebar.caption(
        f"Modelo: {metadata.get('run_id', 'n/d')} | features: {len(metadata.get('features', []))}"
    )
else:
    st.sidebar.caption("Modelo: ainda nao executado")

if detail.empty:
    st.info("Nenhuma linha para o ativo e periodo selecionados.")
    st.stop()

latest = detail.iloc[-1]
anomalies = detail[detail["is_anomaly"]].sort_values("trade_date", ascending=False)

metric_columns = st.columns(5)
metric_columns[0].metric(
    "Ultimo fechamento",
    f"R$ {latest['adjusted_close']:.2f}",
)
metric_columns[1].metric(
    "Retorno do dia",
    f"{latest['daily_return']:.2%}" if pd.notna(latest["daily_return"]) else "n/d",
)
metric_columns[2].metric(
    "Volatilidade 20d",
    (f"{latest['volatility_20d']:.2%}" if pd.notna(latest["volatility_20d"]) else "n/d"),
)
metric_columns[3].metric(
    "Anomalias",
    f"{int(detail['is_anomaly'].sum())}",
)
metric_columns[4].metric(
    "Ultimo pregao",
    latest["trade_date"].date().isoformat(),
)

price = go.Figure()
price.add_trace(
    go.Scatter(
        x=detail["trade_date"],
        y=detail["adjusted_close"],
        mode="lines",
        name="Ajustado",
        line={"color": "#1f4e79", "width": 2},
    )
)
price.add_trace(
    go.Scatter(
        x=detail["trade_date"],
        y=detail["sma_20"],
        mode="lines",
        name="SMA 20",
        line={"color": "#f2a900", "width": 1.2},
    )
)
price.add_trace(
    go.Scatter(
        x=detail["trade_date"],
        y=detail["sma_50"],
        mode="lines",
        name="SMA 50",
        line={"color": "#7f7f7f", "width": 1.2},
    )
)
if not anomalies.empty:
    price.add_trace(
        go.Scatter(
            x=anomalies["trade_date"],
            y=anomalies["adjusted_close"],
            mode="markers",
            name="Anomalia",
            marker={"color": "#d62728", "size": 10, "symbol": "triangle-down"},
            text=[
                f"{row.symbol} | score {row.anomaly_score:.4f}" for row in anomalies.itertuples()
            ],
            hovertemplate="%{x|%Y-%m-%d}<br>%{y:.2f}<br>%{text}<extra></extra>",
        )
    )
price.update_layout(
    title=f"{selected} | preco ajustado e medias moveis",
    template="plotly_white",
    height=470,
    margin={"l": 10, "r": 10, "t": 50, "b": 10},
    legend={"orientation": "h", "y": 1.02, "x": 0},
)
st.plotly_chart(price, width="stretch")

overview_tab, anomalies_tab, model_tab = st.tabs(["Visao geral", "Anomalias", "Modelo"])

with overview_tab:
    overview = summarize_symbols(filtered)
    st.subheader("Resumo por ativo")
    st.dataframe(overview, hide_index=True, width="stretch")

    behavior = go.Figure()
    behavior.add_trace(
        go.Bar(
            x=detail["trade_date"],
            y=detail["volume"],
            name="Volume",
            marker_color="#7aa6c2",
        )
    )
    behavior.add_trace(
        go.Scatter(
            x=detail["trade_date"],
            y=detail["daily_return"],
            name="Retorno diario",
            yaxis="y2",
            line={"color": "#d62728", "width": 1.4},
        )
    )
    behavior.update_layout(
        title="Volume e retorno diario",
        template="plotly_white",
        height=360,
        yaxis={"title": "Volume"},
        yaxis2={
            "title": "Retorno",
            "overlaying": "y",
            "side": "right",
            "tickformat": ".1%",
        },
        legend={"orientation": "h", "y": 1.02, "x": 0},
        margin={"l": 10, "r": 10, "t": 50, "b": 10},
    )
    st.plotly_chart(behavior, width="stretch")

with anomalies_tab:
    top = top_anomalies(filtered, limit=50)
    if top.empty:
        st.info("Nenhuma anomalia no periodo selecionado.")
    else:
        st.subheader("Top anomalias por score")
        st.dataframe(top, hide_index=True, width="stretch")
        st.download_button(
            "Baixar anomalias em CSV",
            data=top.to_csv(index=False).encode("utf-8"),
            file_name=f"mktpulse_anomalias_{selected}.csv",
            mime="text/csv",
        )

        scores = filtered.loc[filtered["anomaly_score"].notna(), "anomaly_score"]
        histogram = go.Figure(
            go.Histogram(
                x=scores,
                nbinsx=40,
                marker_color="#1f4e79",
                name="Score",
            )
        )
        histogram.update_layout(
            title="Distribuicao do score de anomalia",
            xaxis_title="Score (maior = mais anomalo)",
            yaxis_title="Linhas",
            template="plotly_white",
            height=330,
            margin={"l": 10, "r": 10, "t": 50, "b": 10},
        )
        st.plotly_chart(histogram, width="stretch")

with model_tab:
    st.subheader("Modelo de deteccao")
    if metadata:
        st.json(metadata)
    else:
        st.info("Execute `uv run mktpulse detect` para gerar os metadados do modelo.")
    st.markdown(
        """
        **Como ler o resultado**

        - O modelo e um `IsolationForest` com `RobustScaler`.
        - As features sao retornos, volatilidade, drawdown, distancia das medias,
          volume relativo e amplitude do candle.
        - `anomaly_score` maior significa comportamento mais incomum.
        - O gold guarda todas as linhas com features completas e a flag `is_anomaly`,
          nao apenas os outliers.
        - O resultado e exploratorio: anomalia estatistica nao e, por si so, erro
          de dado nem recomendacao de investimento.
        """
    )

st.caption(
    f"Lake: {lake_root} | fonte: {source} | "
    f"periodo: {start_date} a {end_date} | atualizado: {datetime.now():%Y-%m-%d %H:%M}"
)
