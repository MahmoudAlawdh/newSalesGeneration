from uu import Error

import numpy as np
import pandas as pd
from dask import dataframe as dd
from pymongo import DeleteMany

from db.helpers import new_sales_collection
from helpers.sales import derived_fields

area_df = pd.read_excel("./seasonalities.xlsx", "area")
industry_df = pd.read_excel("./seasonalities.xlsx", "industry")
location_type_df = pd.read_excel("./seasonalities.xlsx", "location_type")
product_focus_df = pd.read_excel("./seasonalities.xlsx", "product_focus")


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
        location_type_df, year, month, "location_type", location_type
    )
    industry_seasonality = __filter_df(industry_df, year, month, "industry", industry)
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


def forward_fill():
    df = pd.DataFrame(new_sales_collection.find())

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
        df.loc[:, "Weekday_Store_Sales_area"]
        + df.loc[:, "Weekday_Store_Sales_industry"]
        + df.loc[:, "Weekday_Store_Sales_product_focus"]
        + df.loc[:, "Weekday_Store_Sales_location_type"]
    ) / 4
    df["weekday_delivery_sales_seasonality"] = (
        df.loc[:, "Weekday_Delivery_Sales_area"]
        + df.loc[:, "Weekday_Delivery_Sales_industry"]
        + df.loc[:, "Weekday_Delivery_Sales_product_focus"]
        + df.loc[:, "Weekday_Delivery_Sales_location_type"]
    ) / 4
    df["weekend_store_sales_seasonality"] = (
        df.loc[:, "Weekend_Store_Sales_area"]
        + df.loc[:, "Weekend_Store_Sales_industry"]
        + df.loc[:, "Weekend_Store_Sales_product_focus"]
        + df.loc[:, "Weekend_Store_Sales_location_type"]
    ) / 4
    df["weekend_delivery_sales_seasonality"] = (
        df.loc[:, "Weekend_Delivery_Sales_area"]
        + df.loc[:, "Weekend_Delivery_Sales_industry"]
        + df.loc[:, "Weekend_Delivery_Sales_product_focus"]
        + df.loc[:, "Weekend_Delivery_Sales_location_type"]
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

    def f(df: pd.DataFrame):
        prevRow = {
            "Weekday_Store_Sales": pd.NA,
            "Weekday_Delivery_Sales": pd.NA,
            "Weekend_Delivery_Sales": pd.NA,
            "Weekend_Store_Sales": pd.NA,
        }
        for i, row in df.iterrows():
            if i == 0:
                prevRow = {
                    "Weekday_Store_Sales": pd.NA,
                    "Weekday_Delivery_Sales": pd.NA,
                    "Weekend_Delivery_Sales": pd.NA,
                    "Weekend_Store_Sales": pd.NA,
                }
                continue
            if (
                pd.isna(row.Monthly_Sales)
                and row.Source == "Generated"
                and pd.notna(prevRow["Weekday_Store_Sales"])
            ):
                weekday_delivery_sales_seasonality = (
                    row.weekday_delivery_sales_seasonality
                )
                weekday_store_sales_seasonality = row.weekday_store_sales_seasonality
                weekend_store_sales_seasonality = row.weekend_store_sales_seasonality
                weekend_delivery_sales_seasonality = (
                    row.weekend_delivery_sales_seasonality
                )
                df.at[i, "Weekday_Store_Sales"] = prevRow["Weekday_Store_Sales"] + (
                    prevRow["Weekday_Store_Sales"] * weekday_store_sales_seasonality
                )
                df.at[i, "Weekday_Delivery_Sales"] = prevRow[
                    "Weekday_Delivery_Sales"
                ] + (
                    prevRow["Weekday_Delivery_Sales"]
                    * weekday_delivery_sales_seasonality
                )
                df.at[i, "Weekend_Delivery_Sales"] = prevRow[
                    "Weekend_Delivery_Sales"
                ] + (
                    prevRow["Weekend_Delivery_Sales"]
                    * weekend_delivery_sales_seasonality
                )
                df.at[i, "Weekend_Store_Sales"] = prevRow["Weekend_Store_Sales"] + (
                    prevRow["Weekend_Store_Sales"] * weekend_store_sales_seasonality
                )
            prevRow["Weekday_Store_Sales"] = df.at[i, "Weekday_Store_Sales"]
            prevRow["Weekday_Delivery_Sales"] = df.at[i, "Weekday_Delivery_Sales"]
            prevRow["Weekend_Delivery_Sales"] = df.at[i, "Weekend_Delivery_Sales"]
            prevRow["Weekend_Store_Sales"] = df.at[i, "Weekend_Store_Sales"]
        return df

    print(len(df[(df["Weekday_Store_Sales"].isna())]))
    df = df.groupby("Reference_Full_ID").apply(f)
    print(len(df[df["Weekday_Store_Sales"].isna()]))
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
    df = derived_fields(df)
    new_sales_collection.delete_many({})

    new_sales_collection.insert_many(df.replace({np.nan: None}).to_dict("records"))
    return df
