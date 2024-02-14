import warnings

from fill_with_averages import fill_sales_with_averages
from helpers.sales import generate_all_sales_records as __generate_all_sales_records
from helpers.seasonality_helper import backword_fill, forward_fill
from interpolate import fill_gaps
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
            "Sales_Year",
            "Sales_Month",
            "Product_Focus",
            "Industry_Level_2",
        ]
    )
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


def step_4_seasonality():
    """
    fill rest of the gaps
    """
    batch_count = 500_000
    for i in range(0, 20):
        forward_fill(batch_count * i, batch_count)
    for i in range(0, 20):
        backword_fill(batch_count * i, batch_count)


if __name__ == "__main__":
    """
    Loop through all the ids, find all close sales, with the same industry and location type with x distance
    """
    # step_1_setup()
    step_2_fill_gaps()
    step_4_seasonality()
    step_2_fill_gaps()
    step_3_averages()
    step_4_seasonality()
    step_2_fill_gaps()
