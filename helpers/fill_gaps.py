import json
import logging

import numpy as __np
import pandas as __pd

from db.helpers import new_sales_collection as __new_sales_collection
from db.queries import new_sales_refenrece_ids_with_sales_count as gaps_of_size_n
from db.queries import (
    new_sales_update_single_record as __new_sales_update_single_record,
)

log = logging.getLogger(__name__)


# ---
def check_values_is_None(record: dict):
    return (
        record.get("Weekday_Delivery_Sales") == None
        or record.get("Weekday_Store_Sales") == None
        or record.get("Weekend_Delivery_Sales") == None
        or record.get("Weekend_Store_Sales") == None
    )


# ---


def fill_gaps(sales_start: int, gap_size: int):
    keys = [
        "Weekday_Store_Sales",
        "Weekday_Delivery_Sales",
        "Weekend_Store_Sales",
        "Weekend_Delivery_Sales",
    ]
    count = 0
    ids = list(gaps_of_size_n(sales_start, gap_size))
    ids_len = len(ids)
    for index, i in enumerate(ids):
        try:
            if count % 1000:
                log.info(f"{index}/{ids_len} count:{count}")
            sales = list(
                __new_sales_collection.find(
                    {
                        "Reference_Full_ID": i["_id"],
                    }
                ).sort("Sales_Period")
            )

            check = any(check_values_is_None(record) for record in sales[1:])
            if not check:
                continue
            if not sales:
                continue
            df = __pd.DataFrame(sales)
            interpolation_keys = list(set(df.columns) & set(keys))
            df[interpolation_keys] = df[interpolation_keys].interpolate(
                limit_area="inside"
            )

            for row in df.itertuples(index=False):
                weekday_store_sales = getattr(row, "Weekday_Store_Sales", None)
                weekday_delivery_sales = getattr(row, "Weekday_Delivery_Sales", None)
                weekend_store_sales = getattr(row, "Weekend_Store_Sales", None)
                weekend_delivery_sales = getattr(row, "Weekend_Delivery_Sales", None)
                location_type = row.Location_Type
                industry = row.Industry_Level_2
                product_focus = row.Product_Focus
                area = row.Level_3_Area
                year = row.Sales_Year
                month = row.Sales_Month
                result = __new_sales_update_single_record(
                    row.Reference_Full_ID,
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
                    reason="Fill Gap",
                )
                if result != None:
                    count += 1
        except:
            pass


def fill_gaps_all_in_one(sales_start: int, gap_size: int):
    keys = [
        "Weekday_Store_Sales",
        "Weekday_Delivery_Sales",
        "Weekend_Store_Sales",
        "Weekend_Delivery_Sales",
    ]

    pipeline = [
        {
            "$match": {
                "Sales_Year": {"$gte": sales_start},
                "Reference_Full_ID": {"$ne": None},
            }
        },
        {
            "$set": {
                "period": {
                    "$ifNull": [
                        "$Sales_Period",
                        {
                            "$dateFromParts": {
                                "year": "$Sales_Year",
                                "month": {"$ifNull": ["$Sales_Month", 1]},
                                "day": 1,
                            }
                        },
                    ]
                },
                "has_sales": {"$gt": [{"$ifNull": ["$Monthly_Sales", 0]}, 0]},
            }
        },
        {"$sort": {"Reference_Full_ID": 1, "period": 1}},
        {
            "$setWindowFields": {
                "partitionBy": "$Reference_Full_ID",
                "sortBy": {"period": 1},
                "output": {
                    "prev": {
                        "$shift": {"output": "$has_sales", "by": 1, "default": None}
                    }
                },
            }
        },
        {"$set": {"run_boundary": {"$cond": [{"$ne": ["$has_sales", "$prev"]}, 1, 0]}}},
        {
            "$setWindowFields": {
                "partitionBy": "$Reference_Full_ID",
                "sortBy": {"period": 1},
                "output": {
                    "run_id": {
                        "$sum": "$run_boundary",
                        "window": {"documents": ["unbounded", "current"]},
                    }
                },
            }
        },
        {"$match": {"has_sales": False}},
        {
            "$group": {
                "_id": {"ref": "$Reference_Full_ID", "run": "$run_id"},
                "gap_length": {"$sum": 1},
            }
        },
        {"$match": {"gap_length": {"$gte": 2, "$lte": gap_size}}},
        {"$group": {"_id": None, "refs": {"$addToSet": "$_id.ref"}}},
        # Use the refs to drive the fill
        {
            "$lookup": {
                "from": __new_sales_collection.name,
                "let": {"refs": "$refs"},
                "pipeline": [
                    {"$match": {"$expr": {"$in": ["$Reference_Full_ID", "$$refs"]}}},
                    {
                        "$set": {
                            "period": {
                                "$ifNull": [
                                    "$Sales_Period",
                                    {
                                        "$dateFromParts": {
                                            "year": "$Sales_Year",
                                            "month": {"$ifNull": ["$Sales_Month", 1]},
                                            "day": 1,
                                        }
                                    },
                                ]
                            }
                        }
                    },
                    {"$sort": {"Reference_Full_ID": 1, "period": 1}},
                    {
                        "$fill": {
                            "partitionBy": "$Reference_Full_ID",
                            "sortBy": {"period": 1},
                            "output": {
                                "Weekday_Store_Sales": {"method": "linear"},
                                "Weekday_Delivery_Sales": {"method": "linear"},
                                "Weekend_Store_Sales": {"method": "linear"},
                                "Weekend_Delivery_Sales": {"method": "linear"},
                            },
                        }
                    },
                ],
                "as": "to_fill",
            }
        },
        {"$unwind": "$to_fill"},
        {"$replaceRoot": {"newRoot": "$to_fill"}},
        # Write back filled values
        {
            "$merge": {
                "into": __new_sales_collection.name,
                "on": ["_id"],
                "whenNotMatched": "discard",
                "whenMatched": "merge",
            }
        },
    ]
    print(pipeline)
    list(__new_sales_collection.aggregate(pipeline, allowDiskUse=True))
