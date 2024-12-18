import warnings
from typing import Literal

from db.helpers import new_sales_collection
from fill_with_averages import fill_sales_with_averages
from helpers.sales import generate_all_sales_records as __generate_all_sales_records
from helpers.seasonality_helper import fill
from interpolate import fill_gaps, prophet_forcast
from setup import setup_sales

warnings.filterwarnings("ignore")


def step_1_setup():
    setup_sales()
    __generate_all_sales_records()


def step_2_fill_gaps():
    print("step 2")
    fill_gaps()


def step_3_averages():
    print("step 3")

    fill_sales_with_averages(
        [
            "Level_3_Area",
            "Location_Type",
            "Brand",
            "Product_Focus",
            "Industry_Level_2",
            "Sales_Year",
            "Sales_Month",
        ]
    )
    fill_sales_with_averages(
        [
            "Level_2_Area",
            "Location_Type",
            "Brand",
            "Product_Focus",
            "Industry_Level_2",
            "Sales_Year",
            "Sales_Month",
        ]
    )
    #
    fill_sales_with_averages(
        [
            "Level_3_Area",
            "Location_Type",
            "Industry_Level_2",
            "Product_Focus",
            "Sales_Year",
            "Sales_Month",
        ]
    )
    fill_sales_with_averages(
        [
            "Level_2_Area",
            "Location_Type",
            "Industry_Level_2",
            "Product_Focus",
            "Sales_Year",
            "Sales_Month",
        ]
    )
    fill_sales_with_averages(
        [
            "Level_3_Area",
            "Location_Type",
            "Industry_Level_2",
            "Sales_Year",
            "Sales_Month",
        ]
    )
    fill_sales_with_averages(
        [
            "Level_2_Area",
            "Location_Type",
            "Industry_Level_2",
            "Sales_Year",
            "Sales_Month",
        ]
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
    step_1_setup()
    # step_2_fill_gaps()
    # step_4_seasonality(["Backward"])
    # step_2_fill_gaps()
    # step_3_averages()
    # step_4_seasonality(["Forward", "Backward"])
    # step_2_fill_gaps()
    # prophet_forcast()
