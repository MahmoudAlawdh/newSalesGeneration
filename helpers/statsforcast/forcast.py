from typing import Iterable, List, Literal, Optional

import numpy as np
import pandas as pd
from statsforecast import StatsForecast
from statsforecast.models import AutoARIMA, AutoETS, AutoTheta

from db.queries import new_sales_find
from helpers.types import CountryList

ModelName = Literal["AutoARIMA", "ETS", "Theta"]


def forecast(
    country: CountryList,
    *,
    h: int = 6,
    freq: str = "M",  # "MS" = month start (use "M" if your dates are month-end)
) -> pd.DataFrame:
    """
    Train AutoARIMA, AutoETS, and AutoTheta on NewSales.Sales-like data
    and forecast h months ahead. Always returns ensemble y_hat_mean.

    Returns a DataFrame with columns:
      - unique_id, ds, y_hat_<model>, y_hat_mean, plus metadata columns.
    """

    # ---- Load ----
    raw = pd.DataFrame(new_sales_find(country).to_list())
    if raw is None or len(raw) == 0:
        raise ValueError(f"No data returned for country={country!r}")

    # ---- Normalize schema ----
    required_cols = {"Reference_Full_ID", "Sales_Period", "Monthly_Sales"}
    missing = required_cols.difference(raw.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    df = raw.rename(
        columns={
            "Reference_Full_ID": "unique_id",
            "Sales_Period": "ds",
            "Monthly_Sales": "y",
        }
    ).copy()

    # Ensure correct dtypes & sort
    df["unique_id"] = df["unique_id"].astype(str)
    df["y"] = pd.to_numeric(df["y"], errors="coerce")
    df["ds"] = pd.to_datetime(df["ds"], utc=False)
    df = df.sort_values(["unique_id", "ds"], kind="mergesort")

    # ---- Identify metadata (keep everything else) ----
    meta_cols = [c for c in df.columns if c not in ("unique_id", "ds", "y")]

    # ---- Clean & filter ----
    df = df.dropna(subset=["y"])
    df = df[df["y"].apply(np.isfinite)]

    # ---- Build models (always all 3) ----
    models = [
        AutoARIMA(season_length=12),
    ]

    # ---- Fit + forecast ----
    sf = StatsForecast(
        models=models,
        freq=freq,
        n_jobs=1,  # safer for debugging; set to -1 for parallel
    )
    sf = sf.fit(df[["unique_id", "ds", "y"]])
    fcst = sf.forecast(h=h, df=df[["unique_id", "ds", "y"]]).reset_index(drop=False)

    # ---- Simple ensemble (mean of y_hat_* columns) ----
    yhat_cols = ["AutoARIMA"]
    fcst["y_hat_mean"] = fcst[yhat_cols].mean(axis=1)

    # ---- Attach latest metadata snapshot per series to future rows ----
    last_meta = (
        df.sort_values(["unique_id", "ds"])
        .groupby("unique_id", as_index=False)
        .tail(1)[["unique_id"] + meta_cols]
    )
    fcst = fcst.merge(last_meta, on="unique_id", how="left")

    # Ensure clean dtypes
    for c in yhat_cols + ["y_hat_mean"]:
        fcst[c] = pd.to_numeric(fcst[c], errors="coerce").astype(float)

    return fcst
