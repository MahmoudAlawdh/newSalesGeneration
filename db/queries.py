from db.helpers import gm_sales_collection as __gm_sales_collection
from db.helpers import new_sales_collection as __new_sales_collection


def gm_sales_find(country: str = "Kuwait"):
    return __gm_sales_collection.find(
        {"Level_1_Area": country, "Source": {"$ne": "Algorithm"}}
    )


def new_sales_find_primary_id():
    return __new_sales_collection.find().sort("Primary_ID", -1).limit(1)


def new_sales_delete():
    return __new_sales_collection.delete_many({})


def new_sales_find(country: str | None = None):
    if country is None:
        return __new_sales_collection.find()
    return __new_sales_collection.find({"Level_1_Area": country})


def new_sales_find_by_country_and_reference_full_id(
    country: str, reference_full_id: str
):
    return __new_sales_collection.find(
        {"Reference_Full_ID": reference_full_id, "Level_1_Area": country}
    )


def new_sales_insert(records):
    return __new_sales_collection.insert_many(records, ordered=False)


def new_sales_average_sale():
    return __new_sales_collection.aggregate(
        [
            {
                "$match": {
                    "Monthly_Sales": {"$ne": None},
                    "Industry_Level_2": {"$ne": 0},
                    "Sales_Year": {"$gte": 2016},
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
                    "Level_1_Area": "Kuwait",
                    "$and": [
                        {"Monthly_Sales": {"$ne": None}},
                        {"Monthly_Sales": {"$ne": 0}},
                    ],
                    "Sales_Year": {"$gte": 2016},
                }
            },
            {"$group": {"_id": "$Reference_Full_ID", "fieldN": {"$sum": 1}}},
            {"$match": {"fieldN": {"$gte": 3}}},
            {"$sort": {"fieldN": -1}},
        ]
    )


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

    def fix_none(value):
        if value == None:
            return 0
        return value

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
    if monthly_sales > 0:
        delivery = monthly_delivery_sales / monthly_sales
    if (
        weekday_total_sales < 0
        or weekend_total_sales < 0
        or monthly_delivery_sales < 0
        or monthly_sales <= 0
        or delivery < 0
    ):
        return None

    return __new_sales_collection.update_one(
        {
            "Source": "Generated",
            "Study": "Generated",
            "Researcher": "Mahmoud",
            "Reference_Full_ID": reference_full_id,
            "Sales_Month": month,
            "Sales_Year": year,
            "Location_Type": location_type,
            "Industry_Level_2": industry,
            "Product_Focus": product_focus,
            "Level_3_Area": area,
        },
        {
            "$set": {
                "Weekday_Store_Sales": weekday_store_sales,
                "Weekday_Delivery_Sales": weekday_delivery_sales,
                "Weekend_Store_Sales": weekend_store_sales,
                "Weekend_Delivery_Sales": weekend_delivery_sales,
                "Weekday_Total_Sales": weekday_total_sales,
                "Weekend_Total_Sales": weekend_total_sales,
                "Monthly_Store_Sales": monthly_store_sales,
                "Monthly_Delivery_Sales": monthly_delivery_sales,
                "Monthly_Sales": monthly_sales,
                "Delivery_%": delivery,
            }
        },
    )
