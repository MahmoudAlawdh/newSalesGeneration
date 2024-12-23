import warnings
from typing import List, Literal

from db.helpers import new_sales_collection
from fill_with_averages import Params, fill_sales_with_averages
from helpers.sales import generate_all_sales_records as __generate_all_sales_records
from helpers.seasonality_helper import fill
from interpolate import fill_gaps, prophet_forcast
from setup import setup_sales

warnings.filterwarnings("ignore")


def step_1_setup(country: list[Literal["Kuwait", "Bahrain", "Qatar"]]):
    setup_sales(country)
    __generate_all_sales_records(country)


def step_2_fill_gaps():
    print("step 2")
    fill_gaps()


def step_3_averages(country: list[Literal["Kuwait", "Bahrain", "Qatar"]]):
    print("step 3")
    c: List[List[Params]] = [
        [
            "Location_Type",
            "Brand",
            "Product_Focus",
            "Industry_Level_2",
            "Sales_Year",
            "Sales_Month",
        ],
        [
            "Location_Type",
            "Industry_Level_2",
            "Product_Focus",
            "Sales_Year",
            "Sales_Month",
        ],
        [
            "Location_Type",
            "Industry_Level_2",
            "Sales_Year",
            "Sales_Month",
        ],
    ]
    for i in c:
        q: List[Params] = i.copy()
        w: List[Params] = i.copy()
        q.append("Level_2_Area")
        w.append("Level_3_Area")
        fill_sales_with_averages(
            q,
            country,
        )
        fill_sales_with_averages(
            w,
            country,
        )

    columns: List[Params] = [
        "Location_Type",
        "Level_2_Area",
        "Level_3_Area",
        "Brand",
        "Product_Focus",
        "Industry_Level_2",
        "Location_Type",
    ]
    for i in columns:
        fill_sales_with_averages(
            [
                i,
                "Sales_Year",
                "Sales_Month",
            ],
            country,
        )


def step_4_seasonality(mode: list[Literal["Forward", "Backward"]]):
    """
    fill rest of the gaps
    """
    query = list(
        new_sales_collection.aggregate(
            [{"$group": {"_id": "$Reference_Full_ID", "fieldN": {"$push": "$$ROOT"}}}]
        )
    )
    if "Forward" in mode:
        count = 0
        for i in query:
            count += 1
            print(count, len(query))
            fill(i["fieldN"], "Forward")
    if "Backward" in mode:
        count = 0
        for i in query:
            count += 1
            print(count, len(query))
            fill(i["fieldN"], "Backward")


if __name__ == "__main__":
    """
    Loop through all the ids, find all close sales, with the same industry and location type with x distance
    """
    # step_1_setup(["Qatar", "Bahrain"])
    step_2_fill_gaps()
    step_4_seasonality(["Backward"])
    step_2_fill_gaps()
    step_4_seasonality(["Forward", "Backward"])
    step_3_averages(["Bahrain", "Qatar"])
    step_4_seasonality(["Forward", "Backward"])
    step_2_fill_gaps()
    # prophet_forcast()
