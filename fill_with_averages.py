import random
import time
from math import isnan
from typing import List
from typing import Literal
from typing import Literal as __Literal
from typing import Optional
from typing import Optional as __Optional

from ducks import Dex as __Dex

from db.helpers import new_sales_collection as __new_sales_collection
from db.queries import (
    new_sales_update_single_record as __new_sales_update_single_record,
)
from helpers.types import CountryList

Params = __Literal[
    "Location_Type",
    "Level_1_Area",
    "Level_2_Area",
    "Level_3_Area",
    "Brand",
    "Product_Focus",
    "Industry_Level_2",
    "Sales_Year",
    "Sales_Month",
    "Location_Type",
]


def __get_pipeline(key: List[Params]):
    dict_list = {}
    dict_list_projection = {}
    for i in key:
        dict_list[i] = f"${i}"
        dict_list_projection[i] = f"$_id.{i}"
    return [
        {
            "$match": {
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


def __get_averages_pipeline(GroupBy: Literal["$Reference_Full_ID", "$Brand"]):
    return [
        {"$match": {"original": False}},
        {
            "$group": {
                "_id": GroupBy,
                "Weekday_Store_Sales": {"$avg": "$Weekday_Store_Sales"},
                "Weekday_Delivery_Sales": {"$avg": "$Weekday_Delivery_Sales"},
                "Weekend_Store_Sales": {"$avg": "$Weekend_Store_Sales"},
                "Weekend_Delivery_Sales": {"$avg": "$Weekend_Delivery_Sales"},
            }
        },
    ]


def __average_query(GroupBy: Literal["$Reference_Full_ID", "$Brand"]):
    pipeline = __get_averages_pipeline(GroupBy)
    averages = list(__new_sales_collection.aggregate(pipeline))
    return __Dex(
        averages,
        ["_id"],
    )


def __query(key: List[Params]):
    pipeline = __get_pipeline(key)
    print(pipeline)
    start = time.time()
    averages = list(__new_sales_collection.aggregate(pipeline))
    end = time.time()
    print(f"_query Execution time: {end - start:.4f} seconds")
    return __Dex(
        averages,
        [
            *key,
            "Sales_Year",
            "Sales_Month",
        ],
    )


def adjusted_value(
    val1: Optional[float],
    val2: Optional[float],
    threshold_pct: float,
    tolerance_pct: float = 0.1,
) -> Optional[float]:
    """
    Return val2 if it lies within threshold_pct of val1; otherwise, adjust it so that its percentage difference
    from val1 is randomly around threshold_pct ± tolerance_pct, anchored to val1.

    Args:
        val1: The reference value or None.
        val2: The value to compare and potentially adjust, or None.
        threshold_pct: The maximum allowed percent difference (e.g., 20 for 20%).
        tolerance_pct: Relative tolerance around threshold_pct for randomization (default 0.1 = ±10%).

    Returns:
        The original val2 if within threshold, an adjusted value around val1 if not, or None if both inputs are None.
    """
    # If both are None, nothing to compare
    if val1 is None or val2 is None:
        return val2

    # Use val1 as the anchor, treating None as 0.0
    anchor = val1 if val1 is not None else 0.0
    v2 = val2 if val2 is not None else 0.0

    # For percent calculations, use divisor = anchor unless zero, then fallback to 1.0
    divisor = anchor if anchor != 0 else 1.0

    # Compute current absolute difference and percent difference
    diff = abs(v2 - anchor)
    pct_diff = (diff / abs(divisor)) * 100

    # If within the allowed threshold, return the original value
    if pct_diff <= threshold_pct:
        return v2

    # Otherwise, pick a random pct around the threshold and compute adjusted value
    low = threshold_pct * (1 - tolerance_pct)
    high = threshold_pct * (1 + tolerance_pct)
    new_pct = random.uniform(low, high)
    new_diff = abs(divisor) * (new_pct / 100)

    # Preserve direction of the adjustment
    direction = 1 if (v2 - anchor) >= 0 else -1
    adjusted_val = anchor + direction * new_diff
    return adjusted_val


def __fill(
    dex: __Dex,
    key: List[Params],
    country: CountryList,
    store_dex: __Dex,
    brand_dex: __Dex,
):
    count = 0
    for i in __new_sales_collection.find(
        {
            "Level_1_Area": {"$in": country},
            "Monthly_Sales": None,
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
        data = found_averages
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
            if weekday_store_sales is not None and not isnan(weekday_store_sales):
                weekday_store_sales = int(
                    weekday_store_sales * (1 + random.uniform(-0.06, 0.06))
                )
            if weekday_delivery_sales is not None and not isnan(weekday_delivery_sales):
                weekday_delivery_sales = int(
                    weekday_delivery_sales * (1 + random.uniform(-0.06, 0.06))
                )
            if weekend_store_sales is not None and not isnan(weekend_store_sales):
                weekend_store_sales = int(
                    weekend_store_sales * (1 + random.uniform(-0.06, 0.06))
                )
            if weekend_delivery_sales is not None and not isnan(weekend_delivery_sales):
                weekend_delivery_sales = int(
                    weekend_delivery_sales * (1 + random.uniform(-0.06, 0.06))
                )
            try:
                brand = brand_dex[{"_id": i["Brand"]}]
                store = store_dex[{"_id": i["Reference_Full_ID"]}]
                if brand or store:
                    if brand:
                        brand = brand.pop()
                    else:
                        brand = {}
                    if store:
                        store = store.pop()
                    else:
                        store = {}

                    weekday_store_sales = adjusted_value(
                        (
                            store["Weekday_Store_Sales"]
                            if store["Weekday_Store_Sales"]
                            else brand["Weekday_Store_Sales"]
                        ),
                        weekday_store_sales,
                        50,
                        0.1,
                    )
                    weekday_delivery_sales = adjusted_value(
                        (
                            store["Weekday_Delivery_Sales"]
                            if store["Weekday_Delivery_Sales"]
                            else brand["Weekday_Delivery_Sales"]
                        ),
                        weekday_store_sales,
                        50,
                        0.1,
                    )
                    weekend_store_sales = adjusted_value(
                        (
                            store["Weekend_Store_Sales"]
                            if store["Weekend_Store_Sales"]
                            else brand["Weekend_Store_Sales"]
                        ),
                        weekday_store_sales,
                        50,
                        0.1,
                    )
                    weekend_delivery_sales = adjusted_value(
                        (
                            store["Weekend_Delivery_Sales"]
                            if store["Weekend_Delivery_Sales"]
                            else brand["Weekend_Delivery_Sales"]
                        ),
                        weekday_store_sales,
                        50,
                        0.1,
                    )
                y = __new_sales_update_single_record(
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
                    reason="Averages with keys:" + ",".join(key),
                )
            except Exception as e:
                print(e)
                pass


def fill_sales_with_averages(
    key: List[Params],
    country: CountryList,
):
    print({"key": key, "country": country})
    dex = __query(key)
    store_avg = __average_query("$Reference_Full_ID")
    brand_avg = __average_query("$Brand")
    __fill(dex, key, country, store_avg, brand_avg)
