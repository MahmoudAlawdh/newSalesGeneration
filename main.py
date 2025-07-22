import warnings
from typing import List, Literal

from numpy import mat

from db.helpers import new_sales_collection
from fill_with_averages import Params, fill_sales_with_averages
from helpers.sales import generate_all_sales_records as __generate_all_sales_records
from helpers.seasonality_helper import fill
from interpolate import fill_gaps, prophet_forcast
from setup import setup_sales

warnings.filterwarnings("ignore")
from helpers.types import CountryList


def step_1_setup(country: CountryList):
    setup_sales(country)
    __generate_all_sales_records(country)


def step_2_fill_gaps():
    print("step 2")
    fill_gaps()


def fill_averages(country: CountryList, columns: list[list[Params]]):
    for i in columns:
        l: List[Params] = i + [
            "Sales_Year",
            "Sales_Month",
        ]
        WithOutArea: List[Params] = l.copy()
        WithArea2: List[Params] = l.copy()
        WithArea2.extend(["Level_2_Area"])
        WithArea3: List[Params] = l.copy()
        WithArea3.extend(["Level_2_Area", "Level_3_Area"])

        fill_sales_with_averages(
            WithArea3,
            country,
        )
        fill_sales_with_averages(
            WithArea2,
            country,
        )
        fill_sales_with_averages(
            WithOutArea,
            country,
        )


def step_3_averages(country: CountryList):
    print("step 3")
    c: List[List[Params]] = [
        # [
        #     "Location_Type",
        #     "Brand",
        #     "Product_Focus",
        #     "Industry_Level_2",
        # ],
        [
            "Location_Type",
            "Industry_Level_2",
            "Product_Focus",
        ],
        [
            "Location_Type",
            "Industry_Level_2",
        ],
        [
            "Product_Focus",
            "Industry_Level_2",
        ],
        ["Brand", "Level_1_Area"],
        ["Industry_Level_2", "Level_1_Area"],
        ["Product_Focus", "Level_1_Area"],
        ["Brand"],
        ["Industry_Level_2"],
        ["Product_Focus"],
        ["Location_Type"],
    ]
    fill_averages(country, c)


def step_4_seasonality(mode: list[Literal["Forward", "Backward"]]):
    """
    fill rest of the gaps
    """
    pipeline = [
        {"$match": {"Monthly_Sales": {"$ne": None}}},
        {"$group": {"_id": "$Reference_Full_ID", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gte": 10, "$lte": 120}}},
        {"$project": {"_id": 1}},
    ]
    good_refs = [r["_id"] for r in new_sales_collection.aggregate(pipeline)]
    if not good_refs:
        print("No refs to process")
        return

    for m in mode:
        count = 0
        for ref in good_refs:
            count += 1
            print(count, len(good_refs))
            records = list(
                new_sales_collection.find({"Reference_Full_ID": ref}).sort(
                    "Sales_Period", 1
                )
            )
            fill(
                records,
                m,
            )


def delete_over_million_sales():
    new_sales_collection.update_many(
        {"Monthly_Sales": {"$gt": 500_000}, "original": False},
        {
            "$set": {
                "Monthly_Sales": None,
                "Monthly_Delivery_Sales": None,
                "Monthly_Store_Sales": None,
                "Weekday_Store_Sales": None,
                "Weekday_Delivery_Sales": None,
                "Weekday_Total_Sales": None,
                "Weekend_Store_Sales": None,
                "Weekend_Delivery_Sales": None,
                "Weekend_Total_Sales": None,
            }
        },
    )


if __name__ == "__main__":
    countries: CountryList = [
        "Kuwait",
        # "Bahrain",
        # "Qatar",
        # "Saudi Arabia",
        # "United Arab Emirates",
        # "Oman",
        # "United Kingdom",
    ]
    # step_1_setup(countries)
    # step_2_fill_gaps()
    # fill_averages(
    #     countries,
    #     [
    #         [
    #             "Location_Type",
    #             "Brand",
    #             "Product_Focus",
    #             "Industry_Level_2",
    #         ]
    #     ],
    # )
    # step_2_fill_gaps()

    # step_4_seasonality(["Backward", "Forward"])
    # step_2_fill_gaps()
    step_3_averages(countries)
    step_4_seasonality(["Forward", "Backward"])
    #
    # delete_over_million_sales()
    # #
    # step_2_fill_gaps()
    # step_4_seasonality(["Backward", "Forward"])
    # step_3_averages(countries)
    # step_4_seasonality(["Forward", "Backward"])
    # prophet_forcast()

# 12086
