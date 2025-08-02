from calendar import weekday
from typing import Any, Dict, Literal, Optional

from pandas import isna
from pymongo import UpdateOne

from config import YEAR
from db.helpers import gm_sales_collection as __gm_sales_collection
from db.helpers import gm_stores_collection as __gm_stores_collection
from db.helpers import new_sales_collection as __new_sales_collection
from helpers.types import CountryList


def gm_stores_find(country: CountryList, brand: Optional[list[str]] = None):
    pipeline: Dict[str, Any] = {"Level_1_Area": {"$in": country}}
    if brand:
        pipeline["Brand"] = {"$in": brand}
    return __gm_stores_collection.find(pipeline)


def gm_sales_find(country: CountryList):
    return __gm_sales_collection.find(
        {
            "Level_1_Area": {"$in": country},
            "Monthly_Sales": {"$nin": [0, None]},
            "Sales_Period": {"$ne": None},
            # "Source": {"$nin": ["Algorithm", "Estimate"]},
        }
    )


def new_sales_find_primary_id():
    return __new_sales_collection.find().sort("Primary_ID", -1).limit(1)


def new_sales_delete():
    return __new_sales_collection.delete_many({})


def new_sales_find(country: CountryList):
    if country is None:
        return __new_sales_collection.find()
    return __new_sales_collection.find({"Level_1_Area": {"$in": country}})


def new_sales_find_by_country_and_reference_full_id(
    country: str, reference_full_id: str
):
    return __new_sales_collection.find(
        {
            "Reference_Full_ID": reference_full_id,
        }
    ).limit(1)


def new_sales_insert(records):
    return __new_sales_collection.insert_many(records, ordered=False)


def new_sales_average_sale():
    return __new_sales_collection.aggregate(
        [
            {
                "$match": {
                    "Monthly_Sales": {"$ne": None},
                    "Industry_Level_2": {"$ne": 0},
                    "Sales_Year": {"$gte": YEAR},
                }
            },
            {
                "$group": {
                    "_id": {
                        "area": "$Level_3_Area",
                        "industry": "$Industry_Level_2",
                        "month": "$Sales_Month",
                        "year": "$Sales_Year",
                        "location_Type": "$Location_Type",
                    },
                    "average_sales": {"$avg": "$Monthly_Sales"},
                }
            },
            {
                "$project": {
                    "_id": False,
                    "area": "$_id.area",
                    "industry": "$_id.industry",
                    "month": "$_id.month",
                    "year": "$_id.year",
                    "location_type": "$_id.location_Type",
                    "average_sales": True,
                }
            },
        ]
    )


def new_sales_refenrece_ids_with_sales_count():
    return __new_sales_collection.aggregate(
        [
            {
                "$match": {
                    "Source": {"$ne": "Algorithm"},
                    "Monthly_Sales": {"$nin": [None, 0]},
                    "Sales_Year": {"$gte": YEAR},
                }
            },
            {"$group": {"_id": "$Reference_Full_ID", "fieldN": {"$sum": 1}}},
            {"$match": {"fieldN": {"$gte": 3}}},
            {"$sort": {"fieldN": -1}},
        ]
    )


def safe_to_int(value):
    if value is None:
        return None
    return round(value)


def fix_record(
    reference_full_id,
    year,
    month,
    location_type,
    industry,
    product_focus,
    area,
    weekday_store_sales: None | float,
    weekday_delivery_sales: None | float,
    weekend_store_sales: None | float,
    weekend_delivery_sales: None | float,
):
    if (
        weekday_delivery_sales == None
        and weekday_store_sales == None
        and weekend_store_sales == None
        and weekend_delivery_sales == None
    ):
        return None
    if (
        weekday_delivery_sales == 0
        and weekday_store_sales == 0
        and weekend_delivery_sales == 0
        and weekend_store_sales == 0
    ):
        return None
    if (
        isna(weekday_delivery_sales)
        and isna(weekday_store_sales)
        and isna(weekend_delivery_sales)
        and isna(weekend_store_sales)
    ):
        return None

    def fix_none(value):
        if isna(value):
            return 0
        if type(value) == int or type(value) == float:
            return value
        return 0

    weekday_total_sales = fix_none(weekday_delivery_sales) + fix_none(
        weekday_store_sales
    )
    weekend_total_sales = fix_none(weekend_delivery_sales) + fix_none(
        weekend_store_sales
    )
    monthly_store_sales = (
        fix_none(weekday_store_sales) * 20 + fix_none(weekend_store_sales) * 8
    )
    monthly_delivery_sales = (
        fix_none(weekday_delivery_sales) * 20 + fix_none(weekend_delivery_sales) * 8
    )
    monthly_sales = fix_none(monthly_store_sales) + fix_none(monthly_delivery_sales)
    delivery = 0
    if monthly_sales > 0 and monthly_delivery_sales != None:
        delivery = monthly_delivery_sales / monthly_sales
    if (
        weekday_total_sales < 0
        or weekend_total_sales < 0
        or monthly_delivery_sales < 0
        or monthly_sales < 0
        or delivery < 0
    ):
        return None
    return {
        "reference_full_id": reference_full_id,
        "year": year,
        "month": month,
        "location_type": location_type,
        "industry": industry,
        "product_focus": product_focus,
        "area": area,
        "weekday_store_sales": safe_to_int(fix_none(weekday_store_sales)),
        "weekday_delivery_sales": safe_to_int(fix_none(weekday_delivery_sales)),
        "weekend_store_sales": safe_to_int(fix_none(weekend_store_sales)),
        "weekend_delivery_sales": safe_to_int(fix_none(weekend_delivery_sales)),
        "weekday_total_sales": safe_to_int(weekday_total_sales),
        "weekend_total_sales": safe_to_int(weekend_total_sales),
        "monthly_store_sales": safe_to_int(monthly_store_sales),
        "monthly_delivery_sales": safe_to_int(monthly_delivery_sales),
        "monthly_sales": safe_to_int(monthly_sales),
        "delivery": safe_to_int(delivery),
    }


def new_sales_update_single_record(
    reference_full_id,
    year,
    month,
    location_type,
    industry,
    product_focus,
    area,
    weekday_store_sales: None | float,
    weekday_delivery_sales: None | float,
    weekend_store_sales: None | float,
    weekend_delivery_sales: None | float,
):
    x = fix_record(
        reference_full_id,
        year,
        month,
        location_type,
        industry,
        product_focus,
        area,
        weekday_store_sales,
        weekday_delivery_sales,
        weekend_store_sales,
        weekend_delivery_sales,
    )
    if not x:
        return None
    return __new_sales_collection.update_one(
        {
            "Source": "Generated",
            "Study": "Generated",
            "Researcher": "Mahmoud",
            "Reference_Full_ID": reference_full_id,
            "Sales_Month": month,
            "Sales_Year": year,
        },
        {
            "$set": {
                "Weekday_Store_Sales": weekday_store_sales,
                "Weekday_Delivery_Sales": weekday_delivery_sales,
                "Weekend_Store_Sales": weekend_store_sales,
                "Weekend_Delivery_Sales": weekend_delivery_sales,
                "Weekday_Total_Sales": x["weekday_total_sales"],
                "Weekend_Total_Sales": x["weekend_total_sales"],
                "Monthly_Store_Sales": x["monthly_store_sales"],
                "Monthly_Delivery_Sales": x["monthly_delivery_sales"],
                "Monthly_Sales": x["monthly_sales"],
                "Delivery_%": x["delivery"],
            }
        },
    )


def new_sales_update_many_record(data: list[dict]):
    operations = [
        fix_record(
            i["reference_full_id"],
            i["year"],
            i["month"],
            i["location_type"],
            i["industry"],
            i["product_focus"],
            i["area"],
            i["weekday_store_sales"],
            i["weekday_delivery_sales"],
            i["weekend_store_sales"],
            i["weekend_delivery_sales"],
        )
        for i in data
    ]
    operations = [x for x in operations if x is not None]
    if len(operations) == 0:
        return None
    return __new_sales_collection.bulk_write(
        [
            UpdateOne(
                {
                    "Source": "Generated",
                    "Study": "Generated",
                    "Researcher": "Mahmoud",
                    "Reference_Full_ID": i["reference_full_id"],
                    "Sales_Month": i["month"],
                    "Sales_Year": i["year"],
                    "Location_Type": i["location_type"],
                    "Industry_Level_2": i["industry"],
                    "Product_Focus": i["product_focus"],
                    "Level_3_Area": i["area"],
                },
                {
                    "$set": {
                        "Weekday_Store_Sales": i["weekday_store_sales"],
                        "Weekday_Delivery_Sales": i["weekday_delivery_sales"],
                        "Weekend_Store_Sales": i["weekend_store_sales"],
                        "Weekend_Delivery_Sales": i["weekend_delivery_sales"],
                        "Weekday_Total_Sales": i["weekday_total_sales"],
                        "Weekend_Total_Sales": i["weekend_total_sales"],
                        "Monthly_Store_Sales": i["monthly_store_sales"],
                        "Monthly_Delivery_Sales": i["monthly_delivery_sales"],
                        "Monthly_Sales": i["monthly_sales"],
                        "Delivery_%": i["delivery"],
                    },
                },
            )
            for i in operations
        ]
    )
