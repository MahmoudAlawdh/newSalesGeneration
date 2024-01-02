from db.helpers import gm_sales_collection as __gm_sales_collection
from db.helpers import new_sales_collection as __new_sales_collection


def gm_sales_find(country: str | None = None):
    if country is None:
        return __new_sales_collection.find()
    return __gm_sales_collection.find({"Level_1_Area": country})


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
                    "Location_Type": {"$ne": 0},
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
                    "Level_1_Area": "Kuwait",
                    "$and": [
                        {"Monthly_Sales": {"$ne": None}},
                        {"Monthly_Sales": {"$ne": 0}},
                    ],
                    "Sales_Year": {"$gte": 2016},
                }
            },
            {"$group": {"_id": "$Reference_Full_ID", "fieldN": {"$sum": 1}}},
            {"$match": {"fieldN": {"$gte": 12}}},
            {"$sort": {"fieldN": -1}},
        ]
    )
