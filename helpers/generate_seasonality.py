from datetime import datetime

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from db.helpers import new_sales_collection
from helpers.tables import area_table, industry_table

__keys = [
    "Weekday_Store_Sales",
    "Weekday_Delivery_Sales",
    "Weekend_Store_Sales",
    "Weekend_Delivery_Sales",
]


def __calculate_growth(value1, value2):
    if value1 == 0:
        return None
    else:
        growth = (value2 - value1) / value1
        return growth


def __group_sales(group_id, match):
    pipeline = [
        {
            "$match": {
                **match,
                "Level_1_Area": "Kuwait",
                "Monthly_Sales": {"$nin": [None, 0]},
                "Source": {"$ne": "Algorithm"},
            }
        },
        {
            "$group": {
                "_id": {**group_id, "year": "$Sales_Year", "month": "$Sales_Month"},
                "Weekday_Store_Sales": {"$sum": "$Weekday_Store_Sales"},
                "Weekday_Delivery_Sales": {"$sum": "$Weekday_Delivery_Sales"},
                "Weekend_Store_Sales": {"$sum": "$Weekend_Store_Sales"},
                "Weekend_Delivery_Sales": {"$sum": "$Weekend_Delivery_Sales"},
                "numberOfOutlets": {"$sum": 1},
            }
        },
        {"$sort": {"_id.year": 1, "_id.month": 1}},
    ]
    return new_sales_collection.aggregate(pipeline)


def __generate_seasonality_record(base, data):
    result = {**base}
    for key in __keys:
        if len(data) != 2:
            result[key] = None
            continue
        growth = __calculate_growth(
            data[0][key] / data[0]["numberOfOutlets"],
            data[1][key] / data[1]["numberOfOutlets"],
        )
        if growth:
            if growth < 2 and growth > -1:
                result[key] = growth
    return result


def __getDates(
    start_date: datetime = datetime(2016, 1, 1),
    end_date: datetime = datetime(2023, 12, 1),
):
    date = start_date
    while date <= end_date:
        yield date
        date += relativedelta(months=1)


def __filter_sales(data: list, date_1: datetime, date_2: datetime):
    return [
        record
        for record in data
        if (
            record["_id"]["year"] == date_1.year
            and record["_id"]["month"] == date_1.month
        )
        or (
            record["_id"]["year"] == date_2.year
            and record["_id"]["month"] == date_2.month
        )
    ]


def generate_location_type_seasonality():
    location_types = new_sales_collection.distinct(
        "Location_Type", {"Location_Type": {"$ne": 0}}
    )

    _id = {"Location_type": "$Location_Type"}
    result = []
    for i in location_types:
        for date in __getDates():
            last_month = date - relativedelta(months=1)
            data = __filter_sales(
                list(
                    __group_sales(
                        _id,
                        {
                            "Location_Type": i,
                            "Sales_Month": {"$in": [date.month, last_month.month]},
                            "Sales_Year": {"$in": [date.year, last_month.year]},
                        },
                    )
                ),
                date,
                last_month,
            )
            result.append(
                __generate_seasonality_record(
                    {"location_type": i, "year": date.year, "month": date.month}, data
                )
            )
    for record in result:
        for key in __keys:
            if not key in record or record[key] == None:
                current_date = datetime(record["year"], record["month"], 1)
                last_month = current_date - relativedelta(months=1)
                all_locations_growth = __filter_sales(
                    list(
                        __group_sales(
                            {},
                            {
                                "Sales_Month": {
                                    "$in": [current_date.month, last_month.month]
                                },
                                "Sales_Year": {
                                    "$in": [current_date.year, last_month.year]
                                },
                            },
                        )
                    ),
                    current_date,
                    last_month,
                )
                if len(all_locations_growth) != 2:
                    continue
                first_month = (
                    all_locations_growth[0][key]
                    / all_locations_growth[0]["numberOfOutlets"]
                )
                second_month = (
                    all_locations_growth[1][key]
                    / all_locations_growth[1]["numberOfOutlets"]
                )
                growth = __calculate_growth(first_month, second_month)
                # Check next month, add all_locations_growth to next month growth
                record[key] = growth
    location_type_df = pd.DataFrame(result)
    return location_type_df


def generate_product_type_seasonality():
    products_types = new_sales_collection.distinct(
        "Product_Focus", {"Level_1_Area": "Kuwait", "Product_Focus": {"$ne": 0}}
    )
    _id = {"Product_Focus": "$Product_Focus"}
    result = []
    for i in products_types:
        for date in __getDates():
            last_month = date - relativedelta(months=1)
            data = __filter_sales(
                list(
                    __group_sales(
                        _id,
                        {
                            "Product_Focus": i,
                            "Sales_Month": {"$in": [date.month, last_month.month]},
                            "Sales_Year": {"$in": [date.year, last_month.year]},
                        },
                    )
                ),
                date,
                last_month,
            )
            result.append(
                __generate_seasonality_record(
                    {"product_focus": i, "year": date.year, "month": date.month}, data
                )
            )
    for record in result:
        for key in __keys:
            if key not in record or record[key] == None:
                current_date = datetime(record["year"], record["month"], 1)
                last_month = current_date - relativedelta(months=1)
                all_locations_growth = __filter_sales(
                    list(
                        __group_sales(
                            {},
                            {
                                "Sales_Month": {
                                    "$in": [current_date.month, last_month.month]
                                },
                                "Sales_Year": {
                                    "$in": [current_date.year, last_month.year]
                                },
                            },
                        )
                    ),
                    current_date,
                    last_month,
                )
                if len(all_locations_growth) != 2:
                    # raise Exception("all_locations_growth length issue")
                    continue
                first_month = (
                    all_locations_growth[0][key]
                    / all_locations_growth[0]["numberOfOutlets"]
                )
                second_month = (
                    all_locations_growth[1][key]
                    / all_locations_growth[1]["numberOfOutlets"]
                )
                growth = __calculate_growth(first_month, second_month)
                # Check next month, add all_locations_growth to next month growth
                record[key] = growth
    product_focus_df = pd.DataFrame(result)
    return product_focus_df


def area_seasonality():
    areas = new_sales_collection.distinct("Level_3_Area", {"Level_1_Area": "Kuwait"})
    _id = {"Level_3_Area": "$Level_3_Area"}
    result = []
    for i in areas:
        for date in __getDates():
            last_month = date - relativedelta(months=1)
            data = __filter_sales(
                list(
                    __group_sales(
                        _id,
                        {
                            "Level_3_Area": i,
                            "Sales_Month": {"$in": [date.month, last_month.month]},
                            "Sales_Year": {"$in": [date.year, last_month.year]},
                        },
                    )
                ),
                date,
                last_month,
            )
            result.append(
                __generate_seasonality_record(
                    {"area": i, "year": date.year, "month": date.month}, data
                )
            )
    # check growth for level 2 area
    for record in result:
        for key in __keys:
            if key not in record or record[key] == None:
                area_level_2 = area_table[record["area"]]
                current_date = datetime(record["year"], record["month"], 1)
                last_month = current_date - relativedelta(months=1)
                all_locations_growth = __filter_sales(
                    list(
                        __group_sales(
                            {"Level_2_Area": "$Level_2_Area"},
                            {
                                "Level_2_Area": area_level_2,
                                "Sales_Month": {
                                    "$in": [current_date.month, last_month.month]
                                },
                                "Sales_Year": {
                                    "$in": [current_date.year, last_month.year]
                                },
                            },
                        )
                    ),
                    current_date,
                    last_month,
                )
                if len(all_locations_growth) != 2:
                    continue
                    # raise Exception("all_locations_growth length issue")
                first_month = (
                    all_locations_growth[0][key]
                    / all_locations_growth[0]["numberOfOutlets"]
                )
                second_month = (
                    all_locations_growth[1][key]
                    / all_locations_growth[1]["numberOfOutlets"]
                )
                growth = __calculate_growth(first_month, second_month)
                # Check next month, add all_locations_growth to next month growth
                record[key] = growth
    area_df = pd.DataFrame(result)
    return area_df


def industry_seasonality():
    industry = new_sales_collection.distinct(
        "Industry_Level_2", {"Level_1_Area": "Kuwait", "Industry_Level_2": {"$ne": 0}}
    )
    _id = {"Industry_Level_2": "Industry_Level_2"}
    result = []

    def group_sales_2(group_id, match, industry):
        pipeline = [
            {
                "$match": {
                    **match,
                    "Level_1_Area": "Kuwait",
                    "Monthly_Sales": {"$nin": [None, 0]},
                }
            },
            {
                "$lookup": {
                    "from": "Brands",
                    "localField": "Brand",
                    "foreignField": "Brand_Name_English",
                    "as": "brand",
                    "pipeline": [
                        {
                            "$match": {
                                "Industry_Level_1": industry,
                            },
                        },
                    ],
                }
            },
            {"$match": {"brand.0": {"$exists": True}}},
            {
                "$group": {
                    "_id": {
                        **group_id,
                        "year": "$Sales_Year",
                        "month": "$Sales_Month",
                    },
                    "Weekday_Store_Sales": {"$sum": "$Weekday_Store_Sales"},
                    "Weekday_Delivery_Sales": {"$sum": "$Weekday_Delivery_Sales"},
                    "Weekend_Store_Sales": {"$sum": "$Weekend_Store_Sales"},
                    "Weekend_Delivery_Sales": {"$sum": "$Weekend_Delivery_Sales"},
                    "numberOfOutlets": {"$sum": 1},
                }
            },
            {"$sort": {"_id.year": 1, "_id.month": 1}},
        ]
        try:
            return new_sales_collection.aggregate(pipeline)
        except:
            print(pipeline)
            raise Exception("group_sales_2 error")

    for i in industry:
        for date in __getDates():
            last_month = date - relativedelta(months=1)
            data = __filter_sales(
                list(
                    __group_sales(
                        _id,
                        {
                            "Industry_Level_2": i,
                            "Sales_Month": {"$in": [date.month, last_month.month]},
                            "Sales_Year": {"$in": [date.year, last_month.year]},
                        },
                    )
                ),
                date,
                last_month,
            )
            result.append(
                __generate_seasonality_record(
                    {"industry": i, "year": date.year, "month": date.month}, data
                )
            )

    for record in result:
        for key in __keys:
            if key not in record or record[key] == None:
                industry = industry_table[record["industry"]]
                current_date = datetime(record["year"], record["month"], 1)
                last_month = current_date - relativedelta(months=1)
                all_locations_growth = __filter_sales(
                    list(
                        group_sales_2(
                            {},
                            {
                                "Sales_Month": {
                                    "$in": [current_date.month, last_month.month]
                                },
                                "Sales_Year": {
                                    "$in": [current_date.year, last_month.year]
                                },
                            },
                            industry,
                        )
                    ),
                    current_date,
                    last_month,
                )
                if len(all_locations_growth) != 2:
                    continue
                    # raise Exception("all_locations_growth length issue")
                first_month = (
                    all_locations_growth[0][key]
                    / all_locations_growth[0]["numberOfOutlets"]
                )
                second_month = (
                    all_locations_growth[1][key]
                    / all_locations_growth[1]["numberOfOutlets"]
                )
                growth = __calculate_growth(first_month, second_month)
                # Check next month, add all_locations_growth to next month growth
                record[key] = growth
    industry_df = pd.DataFrame(result)
    return industry_df
