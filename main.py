import warnings
from typing import Optional

import pandas as pd
from ducks import Dex

from db.helpers import new_sales_collection
from db.queries import new_sales_update_single_record
from fill_with_averages import fill_sales_with_averages
from helpers.seasonality_helper import backword_fill, forward_fill
from interpolate import fill_gaps
from setup import setup_sales

warnings.filterwarnings("ignore")


def step_1():
    setup_sales()


def step_2():
    print("step 2")
    fill_gaps()


def step_3():
    """skippable"""
    print("step 3")
    #
    fill_sales_with_averages(
        [
            "Level_3_Area",
            "Location_Type",
            "Brand",
            "Sales_Year",
            "Sales_Month",
            "Product_Focus",
            "Industry_Level_2",
        ]
    )
    step_2()
    fill_sales_with_averages(
        [
            "Level_2_Area",
            "Location_Type",
            "Brand",
            "Sales_Year",
            "Sales_Month",
            "Product_Focus",
            "Industry_Level_2",
        ]
    )
    step_2()
    # #
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
    step_2()
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


def step_4():
    """
    fill rest of the gaps
    """
    forward_fill()
    step_3()
    backword_fill()
    step_3()


if __name__ == "__main__":
    """
    Loop through all the ids, find all close sales, with the same industry and location type with x distance
    """
    # step_1()
    step_2()
    step_3()
    step_4()
