import pandas as pd
from prophet import Prophet


def prophet_forecast_model(df: pd.DataFrame, key):
    tmp = df[[key, "Sales_Period"]].copy()
    tmp = tmp.set_index("Sales_Period")
    tmp = tmp.reset_index()[["Sales_Period", key]].rename(
        columns={"Sales_Period": "ds", key: "y"}
    )
    model = Prophet(
        yearly_seasonality=False,
        seasonality_mode="multiplicative",
        # changepoint_prior_scale=30,
        # seasonality_prior_scale=30,
    )
    return model.fit(tmp)


def get_prediction(model: Prophet, number_of_months):
    future_dates = model.make_future_dataframe(periods=number_of_months, freq="MS")
    predictions = model.predict(future_dates)
    return predictions
