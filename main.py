import logging
import warnings
from typing import List, Literal, Optional

import hydra

from config import Config
from db.helpers import new_sales_collection
from db.queries import derived_fields, exclude_irrelevant_sales, fix_negative_sales
from fill_with_averages import Params, fill_sales_with_averages
from helpers.fill_gaps import fill_gaps
from helpers.seasonality_helper import fill
from helpers.types import CountryList
from interpolate import prophet_forcast
from setup import setup_sales

warnings.filterwarnings("ignore")

log = logging.getLogger(__name__)


def setup(
    country: CountryList,
    sales_start: int,
    sales_end: Optional[int] = None,
    brands: Optional[list[str]] = None,
):
    log.info("Started Setup")
    setup_sales(country, sales_start, sales_end, brands)
    log.info("Ended Setup")


def step_2_fill_gaps(sales_start: int, gap_size: int):
    log.info("Started Fill Gaps")
    fill_gaps(sales_start, gap_size)
    log.info("Ended Fill Gaps")


def fill_averages(
    country: CountryList,
    columns: list[list[Params]],
):
    for i in columns:
        l: List[Params] = i + [
            "Sales_Year",
            "Sales_Month",
        ]
        withOutArea: List[Params] = l.copy()
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
        fill_sales_with_averages(withOutArea, country)


def step_3_averages(country: CountryList):
    print("step 3")
    c: List[List[Params]] = [
        # [
        #     "Location_Type",
        #     "Brand",
        #     "Product_Focus",
        #     "Industry_Level_2",
        # ],
        # [
        #     "Location_Type",
        #     "Industry_Level_2",
        #     "Product_Focus",
        # ],
        # [
        #     "Location_Type",
        #     "Industry_Level_2",
        # ],
        # [
        #     "Product_Focus",
        #     "Industry_Level_2",
        # ],
        # ["Brand", "Level_1_Area"],
        # ["Industry_Level_2", "Level_1_Area"],
        # ["Product_Focus", "Level_1_Area"],
        # ["Brand"],
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
        {
            "$match": {
                "Monthly_Sales": {"$ne": None},
            }
        },
        {"$group": {"_id": "$Reference_Full_ID", "count": {"$sum": 1}}},
        {
            "$match": {
                "count": {
                    "$gte": 10,
                }
            }
        },
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


def removing_bad_Sales():
    delete_over_million_sales()
    ids = new_sales_collection.aggregate([{"$group": {"_id": "$Reference_Full_ID"}}])
    index = 0
    for i in ids:
        index += 1
        print(index)
        avg_result = list(
            new_sales_collection.aggregate(
                [
                    {"$match": {"Reference_Full_ID": i["_id"], "original": True}},
                    {"$group": {"_id": None, "avg": {"$avg": "$Monthly_Sales"}}},
                ]
            )
        )
        if not avg_result:
            continue  # skip if no average found

        average = avg_result[0]["avg"]
        upper_bound = average * 2
        lower_bound = average * 0.5
        new_sales_collection.update_many(
            {
                "Reference_Full_ID": i["_id"],
                "original": False,
                "$or": [
                    {"Monthly_Sales": {"$gt": upper_bound}},
                    {"Monthly_Sales": {"$lt": lower_bound}},
                ],
            },
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


def last_step():
    exclude_irrelevant_sales()
    fix_negative_sales()
    derived_fields()


Kuwait = [
    "ananas",
    "BARTONE",
    "BT by BARTONE",
    "Caribou",
    "GOOD DAY",
    "Joe & The Juice",
    "Pick",
    "Starbucks",
    "Starbucks Reserve",
    "Starbucks Reserve Bar",
    "The Coffee Bean & Tea Leaf",
    "Mr. Holmes",
    "Pret A Manager",
]


@hydra.main(
    config_path=".",
    config_name="config",
    version_base=None,
)
def main(config: Config):
    countries = list(config.countries)
    brands = config.brands
    if brands:
        brands = list(brands)
    # setup(
    #     countries,
    #     config.sales_start,
    #     config.sales_end,
    #     brands,
    # )
    # step_2_fill_gaps(config.sales_start, 4)
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
    # step_2_fill_gaps(config.sales_start, 6)
    step_4_seasonality(["Backward", "Forward"])
    step_3_averages(countries)
    # if brands:
    #     prophet_forcast(brands)
    # last_step()


if __name__ == "__main__":
    main()
