# pip install neuralforecast pandas
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from neuralforecast.core import NeuralForecast
from neuralforecast.models import NHITS

from db.queries import new_sales_find
from helpers.types import CountryList

# ====================== CONFIG ======================
ID_COL = "Reference_Full_ID"  # store ID
DATE_COL = "Sales_Period"  # timestamp
Y_COL = "Monthly_Sales"  # target

CATEGORICAL_STATIC = [
    "Industry_Level_2",
    "Level_1_Area",
    "Level_2_Area",
    "Level_3_Area",
    "Brand",
    "Location_Type",
    "Product_Focus",
]

FREQ = "M"  # monthly data
INPUT = 12  # use past 12 months
MAX_STEPS = 400
SCALER = "robust"
# ====================================================


def load_data(src: str | pd.DataFrame) -> pd.DataFrame:
    """Load sales data from a CSV/Parquet/JSON path or accept an existing DataFrame."""
    if isinstance(src, pd.DataFrame):
        df = src.copy()
    else:
        if src.endswith(".csv"):
            df = pd.read_csv(src)
        elif src.endswith(".parquet"):
            df = pd.read_parquet(src)
        elif src.endswith(".json"):
            df = pd.read_json(src)
        else:
            raise ValueError("Unsupported file type. Use .csv, .parquet, or .json")
    df[DATE_COL] = pd.to_datetime(df[DATE_COL])
    return df


def aggregate_sales(df: pd.DataFrame, freq: str = FREQ) -> pd.DataFrame:
    """Aggregate duplicates to one row per (store, period) and format for NeuralForecast."""
    df_agg = df.groupby([ID_COL, pd.Grouper(key=DATE_COL, freq=freq)], as_index=False)[
        Y_COL
    ].sum()
    y_df = df_agg.rename(columns={ID_COL: "unique_id", DATE_COL: "ds", Y_COL: "y"})
    y_df = y_df.sort_values(["unique_id", "ds"])
    return y_df


def build_static_exog(
    df: pd.DataFrame, categorical_cols: List[str] = CATEGORICAL_STATIC
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Build one-hot encoded static exogenous features, one row per store.
    Uses the last known categorical values per store.
    Returns:
      static_df (unique_id + encoded cols), stat_cols (list of encoded column names)
    """
    static_base = (
        df.sort_values(DATE_COL)
        .groupby(ID_COL, as_index=False)[categorical_cols]
        .last()
        .rename(columns={ID_COL: "unique_id"})
    )

    static_encoded = pd.get_dummies(
        static_base, columns=categorical_cols, dummy_na=True
    )

    stat_cols = [c for c in static_encoded.columns if c != "unique_id"]
    return static_encoded, stat_cols


def build_model(
    h: int = H,
    input_size: int = INPUT,
    max_steps: int = MAX_STEPS,
    scaler_type: str = SCALER,
    stat_exog_list: Optional[List[str]] = None,
    **model_kwargs: Any,
) -> NHITS:
    """Create an NHITS model with optional static exogenous list and extra kwargs."""
    return NHITS(
        h=h,
        input_size=input_size,
        max_steps=max_steps,
        scaler_type=scaler_type,
        stat_exog_list=stat_exog_list or [],
        **model_kwargs,
    )


def fit_model(
    y_df: pd.DataFrame,
    static_df: Optional[pd.DataFrame],
    model: NHITS,
    freq: str = FREQ,
) -> NeuralForecast:
    """Fit a NeuralForecast object with the given model and (optional) static exog."""
    nf = NeuralForecast(models=[model], freq=freq)
    if static_df is not None:
        nf.fit(df=y_df, static_df=static_df)
    else:
        nf.fit(df=y_df)
    return nf


def forecast(
    nf: NeuralForecast,
) -> pd.DataFrame:
    """Predict next H periods per store; returns long DataFrame with columns: unique_id, ds, NHITS."""
    y_hat = nf.predict()
    return y_hat


def train_nhits_per_store(
    data_src: str | pd.DataFrame,
    horizon: int = H,
    input_size: int = INPUT,
    max_steps: int = MAX_STEPS,
    freq: str = FREQ,
    categorical_static: List[str] = CATEGORICAL_STATIC,
    model_kwargs: Optional[Dict[str, Any]] = None,
) -> Tuple[NeuralForecast, pd.DataFrame]:
    """
    One-call convenience function:
      1) load + aggregate
      2) build static exog
      3) build model
      4) fit + forecast
    Returns (nf, forecasts)
    """
    model_kwargs = model_kwargs or {}
    df = load_data(data_src)
    y_df = aggregate_sales(df, freq=freq)
    static_df, stat_cols = build_static_exog(df, categorical_cols=categorical_static)
    model = build_model(
        h=horizon,
        input_size=input_size,
        max_steps=max_steps,
        stat_exog_list=stat_cols,
        **model_kwargs,
    )
    nf = fit_model(y_df, static_df, model, freq=freq)
    y_hat = forecast(nf)
    return nf, y_hat


def nhits(country: CountryList):
    # ---------------------- Example usage ----------------------
    # Example: training directly from your uploaded JSON sample path
    raw = pd.DataFrame(new_sales_find(country).to_list())
    nf, preds = train_nhits_per_store(
        data_src=raw,
        horizon=12,
        input_size=INPUT,
        max_steps=MAX_STEPS,
        model_kwargs=dict(
            # Examples you can tune:
            # n_blocks=[1,1,1],
            # mlp_units=[[256,256],[256,256],[256,256]],
            # dropout_prob_theta=0.1,
        ),
    )

    print(preds.head())
