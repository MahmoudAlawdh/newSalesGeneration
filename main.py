import warnings
from typing import Optional

import pandas as pd
from ducks import Dex

from db.helpers import new_sales_collection
from db.queries import new_sales_update_single_record
from fill_with_averages import fill_sales_with_averages
from helpers.seasonality_helper import forward_fill
from interpolate import fill_gaps
from setup import setup_sales

warnings.filterwarnings("ignore")


def step_1():
    setup_sales()


def step_2():
    fill_gaps()


def step_3():
    """skippable"""
    fill_sales_with_averages("Level_3_Area")
    fill_sales_with_averages("Level_2_Area")
    step_2()


def step_4():
    """
    fill rest of the gaps
    """
    forward_fill()
    step_3()


if __name__ == "__main__":
    """
    Loop through all the ids, find all close sales, with the same industry and location type with x distance
    """
    # step_1()
    # step_2()
    # step_3()
    step_4()

    # reference_ids = list(
    #     new_sales_collection.distinct(
    #         "Reference_Full_ID",
    #         {
    #             "Source": {"$ne": "Generated"},
    #             "Monthly_Sales": {"$ne": None},
    #             "Sales_Year": {"$gte": 2016},
    #             "Level_1_Area": "Kuwait",
    #         },
    #     )
    # )
    # reference_ids_length = len(reference_ids)
    # count = 0
    # c = 0
    # for i in reference_ids:
    #     count += 1
    #     print(count, reference_ids_length, c)
    #     records = list(
    #         new_sales_collection.find({"Reference_Full_ID": i}).sort("Sales_Period")
    #     )
    #     keys = [
    #         "Weekend_Delivery_Sales",
    #         "Weekday_Delivery_Sales",
    #         "Weekend_Store_Sales",
    #         "Weekday_Store_Sales",
    #     ]
    #     df = pd.DataFrame(records)
    #     df = df.replace(0, np.nan)
    #     r = df[df["Monthly_Sales"].isna()].to_dict(orient="records")
    #     if len(df[df["Monthly_Sales"].notna()]) < 2 or len(r) == 0:
    #         continue

    #     weekday_store_forecast = forecast(df, "Weekday_Store_Sales")
    #     weekday_delivery_forecast = forecast(df, "Weekday_Delivery_Sales")
    #     weekend_store_forecast = forecast(df, "Weekend_Store_Sales")
    #     weekend_delivery_forecast = forecast(df, "Weekend_Delivery_Sales")
    #     for i in r:
    #         print(i["Reference_Full_ID"], i["Monthly_Sales"], i["Sales_Period"])
    #         location_type = i["Location_Type"]
    #         industry = i["Industry_Level_2"]
    #         product_focus = i["Product_Focus"]
    #         area = i["Level_3_Area"]
    #         year = i["Sales_Year"]
    #         month = i["Sales_Month"]

    #         weekday_store_sales = (
    #             0
    #             if weekday_store_forecast.empty
    #             else get_data_from_forecast(weekday_store_forecast, year, month)
    #         )
    #         weekday_delivery_sales = (
    #             0
    #             if weekday_delivery_forecast.empty
    #             else get_data_from_forecast(weekday_delivery_forecast, year, month)
    #         )
    #         weekend_store_sales = (
    #             0
    #             if weekend_store_forecast.empty
    #             else get_data_from_forecast(weekend_store_forecast, year, month)
    #         )
    #         weekend_delivery_sales = (
    #             0
    #             if weekend_delivery_forecast.empty
    #             else get_data_from_forecast(weekend_delivery_forecast, year, month)
    #         )
    #         result = new_sales_update_single_record(
    #             i["Reference_Full_ID"],
    #             year,
    #             month,
    #             location_type,
    #             industry,
    #             product_focus,
    #             area,
    #             weekday_store_sales,
    #             weekday_delivery_sales,
    #             weekend_store_sales,
    #             weekend_delivery_sales,
    #         )
    #         if result:
    #             print(result.matched_count)
    #         else:
    #             c += 1
    #             print("No Match")
    #             print(
    #                 weekday_store_sales,
    #                 weekday_delivery_sales,
    #                 weekend_store_sales,
    #                 weekend_delivery_sales,
    #             )
