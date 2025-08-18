from calendar import weekday
from datetime import datetime
from typing import Any, Dict, Literal, Optional

import numpy as np
from pandas import isna
from pymongo import UpdateOne

from db.helpers import gm_sales_collection as __gm_sales_collection
from db.helpers import gm_stores_collection as __gm_stores_collection
from db.helpers import new_sales_collection as __new_sales_collection
from helpers.types import CountryList


def __gm_stores_find(country: CountryList, brand: Optional[list[str]] = None):
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
    return __new_sales_collection.find(
        {"Level_1_Area": {"$in": country}, "Monthly_Sales": {"$ne": None}}
    )


def __new_sales_find_by_country_and_reference_full_id(reference_full_id: str):
    return __new_sales_collection.find(
        {
            "Reference_Full_ID": reference_full_id,
        }
    ).limit(1)


def new_sales_insert(records):
    return __new_sales_collection.insert_many(records, ordered=False)


def new_sales_refenrece_ids_with_sales_count(sales_start: int, gap_size: int):

    return __new_sales_collection.aggregate(
        pipeline=[
            {
                "$match": {
                    "Sales_Period": {"$gte": datetime(sales_start, 1, 1)},
                    "Reference_Full_ID": {"$ne": None},
                }
            },
            # has_sales is True only if *all* fields are not null
            {
                "$addFields": {
                    "has_sales": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$ne": ["$Weekday_Store_Sales", None]},
                                    {"$ne": ["$Weekday_Delivery_Sales", None]},
                                    {"$ne": ["$Weekend_Store_Sales", None]},
                                    {"$ne": ["$Weekend_Delivery_Sales", None]},
                                ]
                            },
                            True,
                            False,
                        ]
                    }
                }
            },
            {"$sort": {"Reference_Full_ID": 1, "Sales_Period": 1}},
            {
                "$setWindowFields": {
                    "partitionBy": "$Reference_Full_ID",
                    "sortBy": {"Sales_Period": 1},
                    "output": {
                        "prev_has_sales": {
                            "$shift": {"output": "$has_sales", "by": 1, "default": None}
                        }
                    },
                }
            },
            {
                "$set": {
                    "run_boundary": {
                        "$cond": [{"$ne": ["$has_sales", "$prev_has_sales"]}, 1, 0]
                    }
                }
            },
            {
                "$setWindowFields": {
                    "partitionBy": "$Reference_Full_ID",
                    "sortBy": {"Sales_Period": 1},
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
                    "gap_start": {"$min": "$Sales_Period"},
                    "gap_end": {"$max": "$Sales_Period"},
                }
            },
            {"$match": {"gap_length": {"$lte": gap_size, "$gt": 1}}},
            {
                "$group": {
                    "_id": "$_id.ref",
                    "gaps": {
                        "$push": {
                            "gap_length": "$gap_length",
                            "gap_start": "$gap_start",
                            "gap_end": "$gap_end",
                        }
                    },
                    "gap_count": {"$sum": 1},
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "Reference_Full_ID": "$_id",
                    "gap_count": 1,
                    "gaps": 1,
                }
            },
            {"$sort": {"Reference_Full_ID": 1}},
        ]
    )


def safe_to_int(value):
    if isna(value):  # catches np.nan, pd.NA, None, NaT
        return None
    if value is None:
        return None
    return int(value)


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
    reason: str,
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
    if location_type in ["Education", "Government"]:
        weekend_delivery_sales = None
        weekend_store_sales = None
        weekday_delivery_sales = None
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
        "reason": reason,
    }


def reject_string(var: Any) -> Any:
    """
    Ensure the provided variable is not a string; otherwise, raise a TypeError.

    Args:
        var: Any value to check.

    Returns:
        The original variable if it is not a string.

    Raises:
        TypeError: If var is an instance of str.
    """
    if isinstance(var, str):
        raise TypeError(f"Expected non-string type, but got string: {var!r}")
    return var


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
    reason,
):
    reject_string(weekday_store_sales)
    reject_string(weekday_delivery_sales)
    reject_string(weekend_store_sales)
    reject_string(weekend_delivery_sales)
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
        reason,
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
            "original": False,
        },
        {
            "$set": {
                "Weekday_Store_Sales": safe_to_int(weekday_store_sales),
                "Weekday_Delivery_Sales": safe_to_int(weekday_delivery_sales),
                "Weekend_Store_Sales": safe_to_int(weekend_store_sales),
                "Weekend_Delivery_Sales": safe_to_int(weekend_delivery_sales),
                "Weekday_Total_Sales": safe_to_int(x["weekday_total_sales"]),
                "Weekend_Total_Sales": safe_to_int(x["weekend_total_sales"]),
                "Monthly_Store_Sales": safe_to_int(x["monthly_store_sales"]),
                "Monthly_Delivery_Sales": safe_to_int(x["monthly_delivery_sales"]),
                "Monthly_Sales": safe_to_int(x["monthly_sales"]),
                "Delivery_%": x["delivery"],
                "reason": reason,
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
            "Seasonality",
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
                        "reason": i["reason"],
                    },
                },
            )
            for i in operations
        ]
    )


def exclude_irrelevant_sales():
    __new_sales_collection.update_many(
        {"Location_Type": {"$in": ["Education", "Government"]}, "original": False},
        [
            {
                "$set": {
                    "Weekend_Delivery_Sales": None,
                    "Weekend_Store_Sales": None,
                    "Weekday_Delivery_Sales": None,
                }
            }
        ],
    )


def fix_negative_sales():
    __new_sales_collection.update_many(
        {"original": False},
        [
            {
                "$set": {
                    "Weekday_Store_Sales": {
                        "$toInt": {"$abs": {"$ifNull": ["$Weekday_Store_Sales", None]}}
                    },
                    "Weekday_Delivery_Sales": {
                        "$toInt": {
                            "$abs": {"$ifNull": ["$Weekday_Delivery_Sales", None]}
                        }
                    },
                    "Weekend_Store_Sales": {
                        "$toInt": {"$abs": {"$ifNull": ["$Weekend_Store_Sales", None]}}
                    },
                    "Weekend_Delivery_Sales": {
                        "$toInt": {
                            "$abs": {"$ifNull": ["$Weekend_Delivery_Sales", None]}
                        }
                    },
                }
            }
        ],
    )


def derived_fields():
    return __new_sales_collection.update_many(
        {"original": False},
        [
            # 1) Compute weekday/weekend totals and monthly (store/delivery) sales,
            #    and convert 0 -> None using $let to avoid recomputing expressions.
            {
                "$set": {
                    "Weekday_Total_Sales": {
                        "$let": {
                            "vars": {
                                "v": {
                                    "$add": [
                                        {"$ifNull": ["$Weekday_Delivery_Sales", 0]},
                                        {"$ifNull": ["$Weekday_Store_Sales", 0]},
                                    ]
                                }
                            },
                            "in": {"$cond": [{"$eq": ["$$v", 0]}, None, "$$v"]},
                        }
                    },
                    "Weekend_Total_Sales": {
                        "$let": {
                            "vars": {
                                "v": {
                                    "$add": [
                                        {"$ifNull": ["$Weekend_Delivery_Sales", 0]},
                                        {"$ifNull": ["$Weekend_Store_Sales", 0]},
                                    ]
                                }
                            },
                            "in": {"$cond": [{"$eq": ["$$v", 0]}, None, "$$v"]},
                        }
                    },
                    "Monthly_Store_Sales": {
                        "$let": {
                            "vars": {
                                "v": {
                                    "$add": [
                                        {
                                            "$multiply": [
                                                {
                                                    "$ifNull": [
                                                        "$Weekday_Store_Sales",
                                                        0,
                                                    ]
                                                },
                                                20,
                                            ]
                                        },
                                        {
                                            "$multiply": [
                                                {
                                                    "$ifNull": [
                                                        "$Weekend_Store_Sales",
                                                        0,
                                                    ]
                                                },
                                                8,
                                            ]
                                        },
                                    ]
                                }
                            },
                            "in": {"$cond": [{"$eq": ["$$v", 0]}, None, "$$v"]},
                        }
                    },
                    "Monthly_Delivery_Sales": {
                        "$let": {
                            "vars": {
                                "v": {
                                    "$add": [
                                        {
                                            "$multiply": [
                                                {
                                                    "$ifNull": [
                                                        "$Weekday_Delivery_Sales",
                                                        0,
                                                    ]
                                                },
                                                20,
                                            ]
                                        },
                                        {
                                            "$multiply": [
                                                {
                                                    "$ifNull": [
                                                        "$Weekend_Delivery_Sales",
                                                        0,
                                                    ]
                                                },
                                                8,
                                            ]
                                        },
                                    ]
                                }
                            },
                            "in": {"$cond": [{"$eq": ["$$v", 0]}, None, "$$v"]},
                        }
                    },
                }
            },
            # 2) Compute Monthly_Sales from freshly computed monthly fields; 0 -> None
            {
                "$set": {
                    "Monthly_Sales": {
                        "$let": {
                            "vars": {
                                "v": {
                                    "$add": [
                                        {"$ifNull": ["$Monthly_Store_Sales", 0]},
                                        {"$ifNull": ["$Monthly_Delivery_Sales", 0]},
                                    ]
                                }
                            },
                            "in": {"$cond": [{"$eq": ["$$v", 0]}, None, "$$v"]},
                        }
                    }
                }
            },
            # 3) Delivery_% based on updated Monthly_Sales; avoid divide-by-zero
            {
                "$set": {
                    "Delivery_%": {
                        "$cond": [
                            {"$gt": [{"$ifNull": ["$Monthly_Sales", 0]}, 0]},
                            {
                                "$divide": [
                                    {"$ifNull": ["$Monthly_Delivery_Sales", 0]},
                                    "$Monthly_Sales",
                                ]
                            },
                            None,
                        ]
                    }
                }
            },
        ],
    )


def delete_zero():
    return __new_sales_collection.update_many(
        {"original": False},
        [
            {
                "$set": {
                    "Weekday_Store_Sales": {
                        "$cond": [
                            {"$eq": ["$Weekday_Store_Sales", 0]},
                            None,
                            "$Weekday_Store_Sales",
                        ]
                    },
                    "Weekday_Delivery_Sales": {
                        "$cond": [
                            {"$eq": ["$Weekday_Delivery_Sales", 0]},
                            None,
                            "$Weekday_Delivery_Sales",
                        ]
                    },
                    "Weekend_Store_Sales": {
                        "$cond": [
                            {"$eq": ["$Weekend_Store_Sales", 0]},
                            None,
                            "$Weekend_Store_Sales",
                        ]
                    },
                    "Weekend_Delivery_Sales": {
                        "$cond": [
                            {"$eq": ["$Weekend_Delivery_Sales", 0]},
                            None,
                            "$Weekend_Delivery_Sales",
                        ]
                    },
                }
            }
        ],
    )
