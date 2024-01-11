from typing import Literal as __Literal
from typing import Optional as __Optional

from ducks import Dex as __Dex

from db.helpers import new_sales_collection as __new_sales_collection
from db.queries import (
    new_sales_update_single_record as __new_sales_update_single_record,
)


def __get_pipeline(key: __Optional[str]):
    if key:
        return [
            {
                "$match": {
                    "Level_1_Area": "Kuwait",
                    "Monthly_Sales": {"$nin": [0, None]},
                    "Location_Type": {"$ne": 0},
                }
            },
            {
                "$group": {
                    "_id": {
                        "Brand": "$Brand",
                        "Location_Type": "$Location_Type",
                        key: f"${key}",
                        "Sales_Year": "$Sales_Year",
                        "Sales_Month": "$Sales_Month",
                    },
                    "Weekday_Store_Sales": {"$avg": "$Weekday_Store_Sales"},
                    "Weekday_Delivery_Sales": {"$avg": "$Weekday_Delivery_Sales"},
                    "Weekend_Store_Sales": {"$avg": "$Weekend_Store_Sales"},
                    "Weekend_Delivery_Sales": {"$avg": "$Weekend_Delivery_Sales"},
                }
            },
            {
                "$match": {
                    "Weekday_Store_Sales": {"$ne": None},
                }
            },
            {
                "$project": {
                    "_id": False,
                    "Brand": "$_id.Brand",
                    "Location_Type": "$_id.Location_Type",
                    key: f"$_id.{key}",
                    "Sales_Year": "$_id.Sales_Year",
                    "Sales_Month": "$_id.Sales_Month",
                    "Weekday_Store_Sales": True,
                    "Weekday_Delivery_Sales": True,
                    "Weekend_Store_Sales": True,
                    "Weekend_Delivery_Sales": True,
                }
            },
        ]
    else:
        return [
            {
                "$match": {
                    "Level_1_Area": "Kuwait",
                    "Monthly_Sales": {"$nin": [0, None]},
                    "Location_Type": {"$ne": 0},
                }
            },
            {
                "$group": {
                    "_id": {
                        "Brand": "$Brand",
                        "Location_Type": "$Location_Type",
                        "Sales_Year": "$Sales_Year",
                        "Sales_Month": "$Sales_Month",
                    },
                    "Weekday_Store_Sales": {"$avg": "$Weekday_Store_Sales"},
                    "Weekday_Delivery_Sales": {"$avg": "$Weekday_Delivery_Sales"},
                    "Weekend_Store_Sales": {"$avg": "$Weekend_Store_Sales"},
                    "Weekend_Delivery_Sales": {"$avg": "$Weekend_Delivery_Sales"},
                }
            },
            {
                "$match": {
                    "Weekday_Store_Sales": {"$ne": None},
                }
            },
            {
                "$project": {
                    "_id": False,
                    "Brand": "$_id.Brand",
                    "Location_Type": "$_id.Location_Type",
                    "Sales_Year": "$_id.Sales_Year",
                    "Sales_Month": "$_id.Sales_Month",
                    "Weekday_Store_Sales": True,
                    "Weekday_Delivery_Sales": True,
                    "Weekend_Store_Sales": True,
                    "Weekend_Delivery_Sales": True,
                }
            },
        ]


def __query(key: __Optional[str]):
    averages = list(__new_sales_collection.aggregate(__get_pipeline(key)))
    if key:
        return __Dex(
            averages,
            [
                "Brand",
                "Location_Type",
                key,
                "Sales_Year",
                "Sales_Month",
            ],
        )
    else:
        return __Dex(
            averages,
            [
                "Brand",
                "Location_Type",
                "Sales_Year",
                "Sales_Month",
            ],
        )


def __fill(dex: __Dex, key: __Optional[str]):
    count = 0
    for i in __new_sales_collection.find(
        {
            "Monthly_Sales": None,
            "Level_1_Area": "Kuwait",
            "Location_Type": {"$ne": 0},
        }
    ):
        year = i["Sales_Year"]
        month = i["Sales_Month"]
        location_type = i["Location_Type"]
        brand = i["Brand"]
        found_averages = None
        if key:
            found_averages = dex[
                {
                    "Brand": brand,
                    "Sales_Year": year,
                    "Sales_Month": month,
                    key: i[key],
                    "Location_Type": location_type,
                }
            ]
        else:
            found_averages = dex[
                {
                    "Brand": brand,
                    "Sales_Year": year,
                    "Sales_Month": month,
                    "Location_Type": location_type,
                }
            ]
        data = [i for i in found_averages if i["Weekday_Store_Sales"] != None]
        if len(data) != 0:
            count += 1
            tmp = data[0]
            industry = i["Industry_Level_2"]
            product_focus = i["Product_Focus"]
            area = i["Level_3_Area"]
            weekday_store_sales = tmp["Weekday_Store_Sales"]
            weekday_delivery_sales = tmp["Weekday_Delivery_Sales"]
            weekend_store_sales = tmp["Weekend_Store_Sales"]
            weekend_delivery_sales = tmp["Weekend_Delivery_Sales"]
            try:
                __new_sales_update_single_record(
                    i["Reference_Full_ID"],
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
            except:
                pass
    print(count)


def fill_sales_with_averages(
    key: __Optional[__Literal["Level_3_Area", "Level_2_Area"]]
):
    dex = __query(key)
    __fill(dex, key)
