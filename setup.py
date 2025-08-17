import logging
import numbers
from datetime import datetime as __datetime
from typing import Optional

from db.helpers import new_sales_collection
from db.queries import (
    __gm_stores_find,
    __new_sales_find_by_country_and_reference_full_id,
)
from db.queries import gm_sales_find as __gm_sales_find
from db.queries import new_sales_delete as __new_sales_delete
from db.queries import new_sales_insert as __new_sales_insert
from helpers.dates import __get_list_of_sales_period
from helpers.dates import get_last_day_of_the_month as __get_last_day_of_the_month
from helpers.sales import (
    get_sales_primary_id_and_reference_ids_set as __get_sales_primary_id_and_reference_ids_set,
)
from helpers.types import CountryList

log = logging.getLogger(__name__)


def __clear_db():
    log.info("Clearing old Data")
    __new_sales_delete()
    log.info("Done Clearing")


def __copy_sales(country: CountryList):
    log.info("Copying Sales")
    records = []
    for i in __gm_sales_find(country):
        location_type = i["Location_Type"]
        industry_level_2 = i["Industry_Level_2"]
        product_focus = i["Product_Focus"]
        brand = str(i["Brand"]).strip()
        company_name = str(i["Company_Name"]).strip()
        sales_period = __get_last_day_of_the_month(i["Sales_Period"])
        records.append(
            {
                **i,
                "Company_Name": company_name,
                "Brand": brand,
                "Sales_Period": sales_period,
                "Sales_Month": sales_period.month,
                "Sales_Year": sales_period.year,
                "Location_Type": location_type if location_type != 0 else None,
                "Industry_Level_2": industry_level_2 if industry_level_2 != 0 else None,
                "Product_Focus": product_focus if product_focus != 0 else None,
                "original": True,
            }
        )
    __new_sales_insert(records)
    log.info("Done Copying Sales")


def __fill_sales_gaps(
    country: CountryList, sales_start: int, sales_end: Optional[int] = None
):
    log.info("Filling Sales Gaps")
    primary_id, reference_ids_set = __get_sales_primary_id_and_reference_ids_set(
        country
    )
    records = []
    count = 0
    for i in reference_ids_set:
        tmp_sales = list(__new_sales_find_by_country_and_reference_full_id(i))
        sample_sales = tmp_sales[0]
        start_date = __opening_date(sample_sales, sales_start)
        end_date = __closing_date(sample_sales, sales_end)
        if end_date < start_date:
            continue
        sales_periods = __get_list_of_sales_period(
            start_date,
            end_date,
        )
        for j in sales_periods:
            primary_id += 1
            records.append(__generate_record(primary_id, sample_sales, j))
            if len(records) > 1_000_000:
                count += len(records)
                __new_sales_insert(records)
                records = []
    if len(records) > 0:
        count += len(records)
        __new_sales_insert(records)
    log.info(f"Done Filling {count} Sales Gaps")


def __generate_all_sales_records(
    country: CountryList, sales_start: int, brand: Optional[list[str]] = None
):
    log.info("Started Generating all sales Records")
    primary_id, _ = __get_sales_primary_id_and_reference_ids_set(country)
    current_year = __datetime.now().year
    current_month = __datetime.now().month
    records = []
    count = 0
    for i in __gm_stores_find(country, brand):

        closing_year = fix_year(int(i.get("Store_Closing_Year", current_year)))
        closing_month = fix_month(int(i.get("Store_Closing_Month", current_month)))
        closing_day = fix_day(int(i.get("Store_Closing_Day", 1)))
        opening_year = fix_year(int(i.get("Store_Opening_Year", sales_start)))
        opening_month = fix_month(int(i.get("Store_Opening_Month", 1)))
        opening_day = fix_day(int(i.get("Store_Opening_Day", 1)))
        sales_periods = __get_list_of_sales_period(
            __datetime(
                int(opening_year if opening_year != 0 else sales_start),
                int(opening_month if opening_month != 0 else 1),
                int(opening_day if opening_day != 0 else 1),
            ),
            __datetime(
                int(closing_year if closing_year != 0 else current_year),
                int(closing_month if closing_month != 0 else current_month),
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
    log.info(f"Done Generating {count} sales Records")


# ---
def __opening_date(i: dict, start_year: int):
    def __check_date(year, month, day):
        if year < start_year:
            year = start_year
        if month == 0:
            month = 1
        if day == 0 or day >= 31:
            day = 1
        return int(year), int(month), int(day)

    opening_year, opening_month, opening_day = (
        i.get("Store_Opening_Year", start_year),
        i.get("Store_Opening_Month", 1),
        i.get("Store_Opening_Day", 1),
    )
    if opening_year == None:
        opening_year = start_year
    if opening_month == None:
        opening_month = 1
    if opening_day == None:
        opening_day = 1
    year, month, day = __check_date(opening_year, opening_month, opening_day)
    return __datetime(year, month, day)


def __closing_date(i, sales_end: Optional[int] = None):
    current_month = __datetime.now().month
    current_year = __datetime.now().year
    if sales_end != None:
        current_month = 12
        current_year = sales_end

    def __check_date(year, month, day):
        if year == 0 or year > 2025:
            year = current_year
        if month == 0:
            month = current_month
        if day == 0 or day >= 31:
            day = 1
        if month == 2 and day > 28:
            day = 28
        return int(year), int(month), int(day)

    closing_year, closing_month, closing_day = (
        i.get("Store_Closing_Year", current_year),
        i.get("Store_Closing_Month", current_month),
        i.get("Store_Closing_Day", 1),
    )
    if closing_year == None:
        closing_year = current_year
    if closing_month == None:
        closing_month = current_month
    if closing_day == None:
        closing_day = 1
    year, month, day = __check_date(closing_year, closing_month, closing_day)
    return __datetime(year, month, day)


def __generate_record(primary_id: int, i: dict, sales_period: __datetime):
    reference_full_id = f'{i.get("Reference_Sheet")} {i["Reference_ID"]}'
    product_focus = i.get("Product_Focus")
    location_type = i.get("Location_Type")
    industry = i.get("Industry_Level_2")
    if isinstance(industry, numbers.Number):
        industry = str(industry)
    return {
        "Primary_ID": primary_id,
        "Primary_Sheet": "Sales",
        "Primary_Full_ID": f"Sales {primary_id}",
        "Reference_ID": i["Reference_ID"],
        "Reference_Sheet": i.get("Reference_Sheet"),
        "Reference_Full_ID": reference_full_id,
        "Company_Name": i.get("Company_Name"),
        "Industry_Level_2": industry,
        "Product_Focus": product_focus if product_focus != 0 else None,
        "Brand": i.get("Brand"),
        "Location_Name": i.get("Location_Name"),
        "Location_Type": location_type if location_type != 0 else None,
        "Location_Name_ID": i.get("Location_Name_ID"),
        "Level_1_Area": i["Level_1_Area"],
        "Level_2_Area": i["Level_2_Area"],
        "Level_3_Area": i["Level_3_Area"],
        "Area_ID": i["Area_ID"],
        "Latitude": i.get("Latitude"),
        "Longitude": i.get("Longitude"),
        "Currency": i.get("Currency"),
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
        "Store_Type": i.get("Store_Type"),
        "Info_Date": __datetime.now(),
        "Source": "Generated",
        "Study": "Generated",
        "Researcher": "Mahmoud",
        "Sales_Month": sales_period.month,
        "Sales_Year": sales_period.year,
        "original": False,
    }


def fix_year(year: int):
    if year > 2025:
        return 2025
    return year


def fix_month(month: int):
    if not month or month > 12 or month == 0:
        return 12
    return month


def fix_day(day: int):
    if not day or day > 28 or day == 0:
        return 1
    return day


# ---


def setup_sales(
    country: CountryList,
    sales_start: int,
    sales_end: Optional[int] = None,
    brands: Optional[list[str]] = None,
):
    __clear_db()
    __copy_sales(country)
    __fill_sales_gaps(country, sales_start, sales_end)
    __generate_all_sales_records(
        country,
        sales_start,
        brands,
    )
    for i in ["Level_2_Area", "Level_3_Area", "Brand"]:
        new_sales_collection.update_many(
            {i: {"$type": "number"}}, [{"$set": {i: {"$toString": f"${i}"}}}]
        )
