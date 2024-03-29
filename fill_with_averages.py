from typing import Literal as __Literal
from typing import Optional as __Optional

from ducks import Dex as __Dex

from db.helpers import new_sales_collection as __new_sales_collection
from db.queries import (
    new_sales_update_single_record as __new_sales_update_single_record,
)

Params = list[
    __Literal[
        "Location_Type",
        "Level_2_Area",
        "Level_3_Area",
        "Brand",
        "Product_Focus",
        "Industry_Level_2",
        "Sales_Year",
        "Sales_Month",
    ]
]


def __get_pipeline(key: Params):
    dict_list = {}
    dict_list_projection = {}
    for i in key:
        dict_list[i] = f"${i}"
        dict_list_projection[i] = f"$_id.{i}"
    return [
        {
            "$match": {
                "Level_1_Area": "Kuwait",
                "$or": [
                    {
                        "Weekday_Store_Sales": {"$ne": None},
                    },
                    {
                        "Weekend_Store_Sales": {"$ne": None},
                    },
                    {
                        "Weekday_Delivery_Sales": {"$ne": None},
                    },
                    {
                        "Weekend_Delivery_Sales": {"$ne": None},
                    },
                ],
            }
        },
        {
            "$group": {
                "_id": {
                    **dict_list,
                },
                "Weekday_Store_Sales": {"$avg": "$Weekday_Store_Sales"},
                "Weekday_Delivery_Sales": {"$avg": "$Weekday_Delivery_Sales"},
                "Weekend_Store_Sales": {"$avg": "$Weekend_Store_Sales"},
                "Weekend_Delivery_Sales": {"$avg": "$Weekend_Delivery_Sales"},
            }
        },
        {
            "$project": {
                "_id": False,
                **dict_list_projection,
                "Weekday_Store_Sales": True,
                "Weekday_Delivery_Sales": True,
                "Weekend_Store_Sales": True,
                "Weekend_Delivery_Sales": True,
            }
        },
    ]


def __query(key: Params):
    averages = list(__new_sales_collection.aggregate(__get_pipeline(key)))
    return __Dex(
        averages,
        [
            *key,
            "Sales_Year",
            "Sales_Month",
        ],
    )


def __fill(
    dex: __Dex,
    key: Params,
):
    count = 0
    for i in __new_sales_collection.find(
        {
            "Monthly_Sales": None,
            "Level_1_Area": "Kuwait",
        }
    ):
        year = i["Sales_Year"]
        month = i["Sales_Month"]
        location_type = i["Location_Type"]
        found_averages = None
        dict_list = {}
        for j in key:
            dict_list[j] = i[j]
        found_averages = dex[
            {
                **dict_list,
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
            except Exception as e:
                print(e)
                pass
    print(count)


def fill_sales_with_averages(key: Params):
    dex = __query(key)
    __fill(dex, key)
