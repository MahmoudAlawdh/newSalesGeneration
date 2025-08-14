import numbers
from datetime import datetime as __datetime
from typing import Optional

import pandas as __pd

from db.queries import __gm_stores_find as __gm_stores_find
from db.queries import new_sales_find as __new_sales_find
from db.queries import new_sales_find_primary_id as __new_sales_find_primary_id
from db.queries import new_sales_insert as __new_sales_insert
from helpers.dates import __get_list_of_sales_period
from helpers.types import CountryList


def get_sales_primary_id_and_reference_ids_set(country: CountryList):
    primary_id: int = 0
    reference_ids_set = set()
    cursor = __new_sales_find(country)

    for i in __new_sales_find_primary_id():
        primary_id = int(i["Primary_ID"])

    for i in cursor:
        reference_ids_set.add(f'{i.get("Reference_Sheet")} {i["Reference_ID"]}')
    return primary_id, reference_ids_set


def derived_fields(df: __pd.DataFrame) -> __pd.DataFrame:
    df = df[
        df["Weekday_Delivery_Sales"].notna()
        | df["Weekday_Store_Sales"].notna()
        | df["Weekend_Delivery_Sales"].notna()
        | df["Weekend_Store_Sales"].notna()
    ]

    df["Weekday_Total_Sales"] = df["Weekday_Delivery_Sales"].fillna(0) + df[
        "Weekday_Store_Sales"
    ].fillna(0)
    df["Weekend_Total_Sales"] = df["Weekend_Delivery_Sales"].fillna(0) + df[
        "Weekend_Store_Sales"
    ].fillna(0)

    df["Monthly_Store_Sales"] = (
        df["Weekday_Store_Sales"].fillna(0) * 20
        + df["Weekend_Store_Sales"].fillna(0) * 8
    )
    df["Monthly_Delivery_Sales"] = (
        df["Weekday_Delivery_Sales"].fillna(0) * 20
        + df["Weekend_Delivery_Sales"].fillna(0) * 8
    )

    df["Monthly_Sales"] = df["Monthly_Store_Sales"] + df["Monthly_Delivery_Sales"]

    df["Delivery_%"] = df["Monthly_Delivery_Sales"] / df["Monthly_Sales"]
    return df
