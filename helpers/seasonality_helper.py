from typing import Literal

import numpy as np
import pandas as pd

from db.helpers import new_sales_collection
from db.queries import new_sales_update_many_record, new_sales_update_single_record

area_df = pd.read_excel("./input/seasonalities.xlsx", "area")
industry_df = pd.read_excel("./input/seasonalities.xlsx", "industry")
location_type_df = pd.read_excel("./input/seasonalities.xlsx", "location_type")
product_focus_df = pd.read_excel("./input/seasonalities.xlsx", "product_focus")

area_reverse_df = pd.read_excel("./input/seasonalities_reverse.xlsx", "area")
industry_reverse_df = pd.read_excel("./input/seasonalities_reverse.xlsx", "industry")
location_type_reverse_df = pd.read_excel(
    "./input/seasonalities_reverse.xlsx", "location_type"
)
product_focus_reverse_df = pd.read_excel(
    "./input/seasonalities_reverse.xlsx", "product_focus"
)


def __filter_df(df: pd.DataFrame, year, month, key: str, value: str):
    return df[(df[key] == value) & (df["year"] == year) & (df["month"] == month)]


def __get_single_seasonality(df: list[pd.DataFrame], key):
    count = 0
    tmp = 0
    for i in df:
        if not i.empty:
            count += 1
            tmp += i[key].values[0]
    if count == 0:
        return 0
    return tmp / count


def __get_seasonalities(year, month, location_type, industry, product_focus, area):
    location_seasonality = __filter_df(
        location_type_df, year, month, "Location_Type", location_type
    )
    industry_seasonality = __filter_df(industry_df, year, month, "Industry", industry)
    product_seasonality = __filter_df(
        product_focus_df, year, month, "product_focus", product_focus
    )
    area_seasonality = __filter_df(area_df, year, month, "area", area)
    Weekday_Store_Sales = __get_single_seasonality(
        [
            location_seasonality,
            industry_seasonality,
            product_seasonality,
            area_seasonality,
        ],
        "Weekday_Store_Sales",
    )
    Weekday_Delivery_Sales = __get_single_seasonality(
        [
            location_seasonality,
            industry_seasonality,
            product_seasonality,
            area_seasonality,
        ],
        "Weekday_Delivery_Sales",
    )
    Weekend_Store_Sales = __get_single_seasonality(
        [
            location_seasonality,
            industry_seasonality,
            product_seasonality,
            area_seasonality,
        ],
        "Weekend_Store_Sales",
    )
    Weekend_Delivery_Sales = __get_single_seasonality(
        [
            location_seasonality,
            industry_seasonality,
            product_seasonality,
            area_seasonality,
        ],
        "Weekend_Store_Sales",
    )
    return (
        Weekday_Store_Sales,
        Weekday_Delivery_Sales,
        Weekend_Store_Sales,
        Weekend_Delivery_Sales,
    )


def setup_seasonalities(
    df: pd.DataFrame,
    area_df: pd.DataFrame,
    industry_df: pd.DataFrame,
    location_type_df: pd.DataFrame,
    product_focus_df: pd.DataFrame,
):
    df = df.merge(
        area_df,
        how="left",
        on=["Level_3_Area", "Sales_Year", "Sales_Month"],
        suffixes=("", "_area"),
    )
    df = df.merge(
        industry_df,
        how="left",
        on=["Industry_Level_2", "Sales_Year", "Sales_Month"],
        suffixes=("", "_industry"),
    )
    df = df.merge(
        location_type_df,
        how="left",
        on=["Location_Type", "Sales_Year", "Sales_Month"],
        suffixes=("", "_location_type"),
    )
    df = df.merge(
        product_focus_df,
        how="left",
        on=["Product_Focus", "Sales_Year", "Sales_Month"],
        suffixes=("", "_product_focus"),
    )

    df["weekday_store_sales_seasonality"] = (
        df.loc[:, "Weekday_Store_Sales_area"].replace(np.nan, 0)
        + df.loc[:, "Weekday_Store_Sales_industry"].replace(np.nan, 0)
        + df.loc[:, "Weekday_Store_Sales_product_focus"].replace(np.nan, 0)
        + df.loc[:, "Weekday_Store_Sales_location_type"].replace(np.nan, 0)
    ) / 4

    df["weekday_delivery_sales_seasonality"] = (
        df.loc[:, "Weekday_Delivery_Sales_area"].replace(np.nan, 0)
        + df.loc[:, "Weekday_Delivery_Sales_industry"].replace(np.nan, 0)
        + df.loc[:, "Weekday_Delivery_Sales_product_focus"].replace(np.nan, 0)
        + df.loc[:, "Weekday_Delivery_Sales_location_type"].replace(np.nan, 0)
    ) / 4
    df["weekend_store_sales_seasonality"] = (
        df.loc[:, "Weekend_Store_Sales_area"].replace(np.nan, 0)
        + df.loc[:, "Weekend_Store_Sales_industry"].replace(np.nan, 0)
        + df.loc[:, "Weekend_Store_Sales_product_focus"].replace(np.nan, 0)
        + df.loc[:, "Weekend_Store_Sales_location_type"].replace(np.nan, 0)
    ) / 4
    df["weekend_delivery_sales_seasonality"] = (
        df.loc[:, "Weekend_Delivery_Sales_area"].replace(np.nan, 0)
        + df.loc[:, "Weekend_Delivery_Sales_industry"].replace(np.nan, 0)
        + df.loc[:, "Weekend_Delivery_Sales_product_focus"].replace(np.nan, 0)
        + df.loc[:, "Weekend_Delivery_Sales_location_type"].replace(np.nan, 0)
    ) / 4

    df.drop(
        [
            "Weekday_Store_Sales_area",
            "Weekday_Delivery_Sales_area",
            "Weekend_Store_Sales_area",
            "Weekend_Delivery_Sales_area",
            #
            "Weekday_Store_Sales_industry",
            "Weekday_Delivery_Sales_industry",
            "Weekend_Store_Sales_industry",
            "Weekend_Delivery_Sales_industry",
            #
            "Weekday_Store_Sales_location_type",
            "Weekday_Delivery_Sales_location_type",
            "Weekend_Store_Sales_location_type",
            "Weekend_Delivery_Sales_location_type",
            #
            "Weekday_Store_Sales_product_focus",
            "Weekday_Delivery_Sales_product_focus",
            "Weekend_Store_Sales_product_focus",
            "Weekend_Delivery_Sales_product_focus",
        ],
        axis=1,
        inplace=True,
    )

    df = df.replace(0, np.nan)

    return df


def update_records(df: pd.DataFrame):
    df = df.replace({np.nan: None})
    records = df.to_dict(orient="records")
    results = []
    for i in records:
        reference_full_id = i["Reference_Full_ID"]
        year = i["Sales_Year"]
        month = i["Sales_Month"]
        location_type = i["Location_Type"]
        industry = i["Industry_Level_2"]
        product_focus = i["Product_Focus"]
        area = i["Level_3_Area"]
        weekday_store_sales = i["Weekday_Store_Sales"]
        weekday_delivery_sales = i["Weekday_Delivery_Sales"]
        weekend_store_sales = i["Weekend_Store_Sales"]
        weekend_delivery_sales = i["Weekend_Delivery_Sales"]
        results.append(
            {
                "reference_full_id": reference_full_id,
                "year": year,
                "month": month,
                "location_type": location_type,
                "industry": industry,
                "product_focus": product_focus,
                "area": area,
                "weekday_store_sales": weekday_store_sales,
                "weekday_delivery_sales": weekday_delivery_sales,
                "weekend_store_sales": weekend_store_sales,
                "weekend_delivery_sales": weekend_delivery_sales,
                "reason": "Seasonality",
            }
        )
    new_sales_update_many_record(results)
    return df


def f(df: pd.DataFrame, tolerance: float = 0.2):
    sales_cols = [
        "Weekday_Store_Sales",
        "Weekday_Delivery_Sales",
        "Weekend_Delivery_Sales",
        "Weekend_Store_Sales",
    ]
    seasonality_cols = [
        "weekday_store_sales_seasonality",
        "weekday_delivery_sales_seasonality",
        "weekend_store_sales_seasonality",
        "weekend_delivery_sales_seasonality",
    ]
    shifted_sales = df[sales_cols].shift()
    mask = (
        df["Monthly_Sales"].isna()
        & (df["Source"] == "Generated")
        & shifted_sales.notna().any(axis=1)
    )

    for sales_col, seasonality_col in zip(sales_cols, seasonality_cols):
        valid = mask & shifted_sales[sales_col].notna() & df[seasonality_col].notna()
        df.loc[valid, sales_col] = shifted_sales.loc[valid, sales_col] * (
            1 + df.loc[valid, seasonality_col]
        )
    df.loc[mask, "changed"] = True

    for col in sales_cols:
        # baseline mean (only original/non-generated data)
        baseline_mean = df.loc[df["Source"] != "Generated", col].mean()
        # mask of generated rows that are now present AND too-far from mean
        outlier_mask = (
            (df["Source"] == "Generated")
            & df[col].notna()
            & (df[col].sub(baseline_mean).abs() > tolerance * baseline_mean)
        )
        # clear them
        df.loc[outlier_mask, col] = np.nan
        # optionally, reset your flag
        df.loc[outlier_mask, "changed"] = False
    return df


def get_seasonalities(
    mode: Literal["Forward", "Backward"],
):
    if mode == "Forward":
        return area_df, industry_df, location_type_df, product_focus_df
    if mode == "Backward":
        return (
            area_reverse_df,
            industry_reverse_df,
            location_type_reverse_df,
            product_focus_reverse_df,
        )


(
    seasonality_area_df,
    seasonality_industry_df,
    seasonality_location_type_df,
    seasonality_product_focus_df,
) = get_seasonalities(
    "Forward",
)
(
    seasonality_backward_area_df,
    seasonality_backward_industry_df,
    seasonality_backward_location_type_df,
    seasonality_backward_product_focus_df,
) = get_seasonalities(
    "Backward",
)


def fill(data, mode: Literal["Forward", "Backward"]):
    if len(data) == 0:
        return None
    df = pd.DataFrame(data)

    if mode == "Forward":
        df = setup_seasonalities(
            df,
            seasonality_area_df,
            seasonality_industry_df,
            seasonality_location_type_df,
            seasonality_product_focus_df,
        )
    else:
        df = setup_seasonalities(
            df,
            seasonality_backward_area_df,
            seasonality_backward_industry_df,
            seasonality_backward_location_type_df,
            seasonality_backward_product_focus_df,
        )
    if mode == "Forward":
        df = df.groupby("Reference_Full_ID").apply(f)
    if mode == "Backward":
        df = df.iloc[::-1]
        df = df.groupby("Reference_Full_ID").apply(f)
        df = df.iloc[::-1]

    #
    df.drop(
        [
            "weekday_store_sales_seasonality",
            "weekday_delivery_sales_seasonality",
            "weekend_store_sales_seasonality",
            "weekend_delivery_sales_seasonality",
        ],
        axis=1,
        inplace=True,
    )
    if "changed" in df.columns:
        changed_len = len(df[df["changed"] == True])
        if changed_len > 0:
            print("changed", changed_len)
            update_records(df[df["changed"] == True])
    return df
