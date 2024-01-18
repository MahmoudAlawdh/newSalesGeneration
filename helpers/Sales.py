from datetime import datetime as __datetime

import pandas as __pd

from db.queries import gm_sales_find as __gm_sales_find
from db.queries import new_sales_delete as __new_sales_delete
from db.queries import new_sales_find as __new_sales_find
from db.queries import (
    new_sales_find_by_country_and_reference_full_id as __new_sales_find_by_country_and_reference_full_id,
)
from db.queries import new_sales_find_primary_id as __new_sales_find_primary_id
from db.queries import new_sales_insert as __new_sales_insert
from helpers.dates import get_last_day_of_the_month, get_list_of_sales_period


def get_sales_primary_id_and_reference_ids_set(country: str | None = None):
    print("Getting Sales Primary ID and Reference IDs Set")
    primary_id: int = 0
    reference_ids_set = set()
    cursor = __new_sales_find(country)

    for i in __new_sales_find_primary_id():
        primary_id = int(i["Primary_ID"])

    for i in cursor:
        reference_ids_set.add(f'{i.get("Reference_Sheet")} {i["Reference_ID"]}')

    print("Done Getting Sales Primary ID and Reference IDs Set")
    return primary_id, reference_ids_set


def copy_sales():
    print("Copying Sales")
    records = []
    for i in __gm_sales_find():
        location_type = i["Location_Type"]
        records.append(
            {
                **i,
                "Sales_Period": get_last_day_of_the_month(i["Sales_Period"]),
                "Location_Type": location_type if location_type != 0 else None,
            }
        )
    __new_sales_insert(records)
    print("Done Copying Sales")


def clear_db():
    print("Clearing old Data")
    __new_sales_delete()
    print("Done Clearing")


def __generate_record(primary_id: int, i, sales_period: __datetime):
    reference_full_id = f'{i.get("Reference_Sheet")} {i["Reference_ID"]}'
    return {
        "Primary_ID": primary_id,
        "Primary_Sheet": "Sales",
        "Primary_Full_ID": f"Sales {primary_id}",
        "Reference_ID": i["Reference_ID"],
        "Reference_Sheet": i.get("Reference_Sheet"),
        "Reference_Full_ID": reference_full_id,
        "Company_Name": i.get("Company_Name"),
        "Industry_Level_2": i.get("Industry_Level_2"),
        "Product_Focus": i.get("Product_Focus"),
        "Brand": i.get("Brand"),
        "Location_Name": i.get("Location_Name"),
        "Location_Type": i.get("Location_Type"),
        "Location_Name_ID": i.get("Location_Name_ID"),
        "Level_1_Area": i["Level_1_Area"],
        "Level_2_Area": i["Level_2_Area"],
        "Level_3_Area": i["Level_3_Area"],
        "Area_ID": i["Area_ID"],
        "Latitude": i.get("Latitude"),
        "Longitude": i.get("Longitude"),
        "Currency": None,
        "Weekday_Store_Sales": None,
        "Weekday_Delivery_Sales": None,
        "Weekday_Total_Sales": None,
        "Weekend_Store_Sales": None,
        "Weekend_Delivery_Sales": None,
        "Weekend_Total_Sales": None,
        "Monthly_Store_Sales": None,
        "Monthly_Delivery_Sales": None,
        "Monthly_Sales": None,
        "Delivery_%": None,
        "Confidence_Rate": 2,
        "Sales_Period": sales_period,
        "Store_Status": "Operational",
        "Store_Opening_Day": i.get("Store_Opening_Day"),
        "Store_Opening_Month": i.get("Store_Opening_Month"),
        "Store_Opening_Year": i.get("Store_Opening_Year"),
        "Store_Closing_Day": i.get("Store_Closing_Day"),
        "Store_Closing_Month": i.get("Store_Closing_Month"),
        "Store_Closing_Year": i.get("Store_Closing_Year"),
        "Info_Date": __datetime.now(),
        "Source": "Generated",
        "Study": "Generated",
        "Researcher": "Mahmoud",
        "Sales_Month": sales_period.month,
        "Sales_Year": sales_period.year,
    }


def generate_all_sales_records():
    print("Generating all sales Records")
    primary_id, reference_ids_set = get_sales_primary_id_and_reference_ids_set(
        country="Kuwait"
    )
    records = []
    count = 0
    for i in __gm_sales_find("Kuwait"):
        reference_full_id = f'{i.get("Reference_Sheet")} {i["Reference_ID"]}'
        if reference_full_id in reference_ids_set:
            continue
        closing_year = i.get("Store_Closing_Year", 2023)
        closing_month = i.get("Store_Closing_Month", 12)
        closing_day = i.get("Store_Closing_Day", 1)
        opening_year = i.get("Store_Opening_Year", 2016)
        opening_month = i.get("Store_Opening_Month", 1)
        opening_day = i.get("Store_Opening_Day", 1)
        sales_periods = get_list_of_sales_period(
            __datetime(
                int(opening_year if opening_year != 0 else 2016),
                int(opening_month if opening_month != 0 else 1),
                int(opening_day if opening_day != 0 else 1),
            ),
            __datetime(
                int(closing_year if closing_year != 0 else 2023),
                int(closing_month if closing_month != 0 else 12),
                int(closing_day if closing_day != 0 else 1),
            ),
        )
        # for sales
        for j in sales_periods:
            primary_id += 1
            records.append(__generate_record(primary_id, i, j))
        if len(records) > 1_000_000:
            count += len(records)
            print(f"Writing {len(records)}")
            __new_sales_insert(records)
            records = []
    if records:
        __new_sales_insert(records)
        records = []
    print(f"Done Generating {count} sales Records")


def __opening_date(i: dict):
    def __check_date(year, month, day):
        if year < 2016:
            year = 2016
        if month == 0:
            month = 1
        if day == 0:
            day = 1
        return int(year), int(month), int(day)

    opening_year, opening_month, opening_day = (
        i.get("Store_Opening_Year", 2016),
        i.get("Store_Opening_Month", 1),
        i.get("Store_Opening_Day", 1),
    )
    if opening_year == None:
        opening_year = 2016
    if opening_month == None:
        opening_month = 1
    if opening_day == None:
        opening_day = 1
    year, month, day = __check_date(opening_year, opening_month, opening_day)
    return __datetime(year, month, day)


def __closing_date(i):
    def __check_date(year, month, day):
        if year == 0:
            year = 2023
        if month == 0:
            month = 12
        if day == 0:
            day = 1
        if month == 2 and day > 28:
            day = 28
        return int(year), int(month), int(day)

    closing_year, closing_month, closing_day = (
        i.get("Store_Closing_Year", 2023),
        i.get("Store_Closing_Month", 12),
        i.get("Store_Closing_Day", 1),
    )
    if closing_year == None:
        closing_year = 2023
    if closing_month == None:
        closing_month = 1
    if closing_day == None:
        closing_day = 1
    year, month, day = __check_date(closing_year, closing_month, closing_day)
    return __datetime(year, month, day)


def fill_sales_gaps():
    print("Filling sales gaps")
    primary_id, reference_ids_set = get_sales_primary_id_and_reference_ids_set("Kuwait")
    records = []
    count = 0
    for i in reference_ids_set:
        tmp_sales = list(__new_sales_find_by_country_and_reference_full_id("Kuwait", i))
        sample_sales = tmp_sales[0]
        start_date = __opening_date(sample_sales)
        end_date = __closing_date(sample_sales)
        if end_date < start_date:
            continue
        sales_periods = get_list_of_sales_period(
            start_date,
            end_date,
        )
        for j in sales_periods:
            primary_id += 1
            records.append(__generate_record(primary_id, sample_sales, j))
            if len(records) > 1_000_000:
                count += len(records)
                print(f"Writing {len(records)}")
                __new_sales_insert(records)
                records = []
    if len(records) > 0:
        count += len(records)
        __new_sales_insert(records)
    print(f"Done Filling {count} sales gaps")


def derived_fields(df: __pd.DataFrame) -> __pd.DataFrame:
    for j in [
        "Weekend_Delivery_Sales",
        "Weekend_Store_Sales",
        "Weekday_Delivery_Sales",
        "Weekday_Store_Sales",
    ]:
        df[j] = df[j].fillna(0)
    df["Weekday_Total_Sales"] = df["Weekday_Delivery_Sales"] + df["Weekday_Store_Sales"]
    df["Weekend_Total_Sales"] = df["Weekend_Delivery_Sales"] + df["Weekend_Store_Sales"]
    df["Monthly_Store_Sales"] = (
        df["Weekday_Store_Sales"] * 20 + df["Weekend_Store_Sales"] * 8
    )
    df["Monthly_Delivery_Sales"] = (
        df["Weekday_Delivery_Sales"] * 20 + df["Weekend_Delivery_Sales"] * 8
    )
    df["Monthly_Sales"] = df["Monthly_Store_Sales"] + df["Monthly_Delivery_Sales"]
    df["Delivery_%"] = df["Monthly_Delivery_Sales"] / df["Monthly_Sales"]
    return df
