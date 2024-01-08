import warnings

import numpy as np
import pandas as pd
from prophet import Prophet
from prophet.plot import plot_components_plotly, plot_plotly

from db.helpers import new_sales_collection
from db.queries import (
    new_sales_average_sale,
    new_sales_refenrece_ids_with_sales_count,
    new_sales_update_single_record,
)
from helpers.Sales import (
    clear_db,
    copy_sales,
    fill_sales_gaps,
    generate_all_sales_records,
    get_sales_primary_id_and_reference_ids_set,
)

warnings.filterwarnings("ignore")
area_df = pd.read_excel("./seasonalities.xlsx", "area")
industry_df = pd.read_excel("./seasonalities.xlsx", "industry")
location_type_df = pd.read_excel("./seasonalities.xlsx", "location_type")
product_focus_df = pd.read_excel("./seasonalities.xlsx", "product_focus")


def prophet_forecast_model(df: pd.DataFrame, key):
    tmp = df[[key, "Sales_Period"]].copy()
    tmp = tmp.set_index("Sales_Period")
    tmp = tmp.reset_index()[["Sales_Period", key]].rename(
        columns={"Sales_Period": "ds", key: "y"}
    )
    model = Prophet()

    return model.fit(tmp)


def get_prediction(model: Prophet, number_of_months):
    future_dates = model.make_future_dataframe(periods=number_of_months, freq="M")
    predictions = model.predict(future_dates)
    return predictions


def setup_sales():
    # clear_db()
    # copy_sales()
    # fill_sales_gaps()
    generate_all_sales_records()


def filter_df(df: pd.DataFrame, year, month, key: str, value: str):
    return df[(df[key] == value) & (df["year"] == year) & (df["month"] == month)]


def get_single_seasonality(df: list[pd.DataFrame], key):
    count = 0
    tmp = 0
    for i in df:
        if not i.empty:
            count += 1
            tmp += i[key].values[0]
    if count == 0:
        return 0
    return tmp / count


def get_seasonalities(year, month, location_type, industry, product_focus, area):
    location_seasonality = filter_df(
        location_type_df, year, month, "location_type", location_type
    )
    industry_seasonality = filter_df(industry_df, year, month, "industry", industry)
    product_seasonality = filter_df(
        product_focus_df, year, month, "product_focus", product_focus
    )
    area_seasonality = filter_df(area_df, year, month, "area", area)
    Weekday_Store_Sales = get_single_seasonality(
        [
            location_seasonality,
            industry_seasonality,
            product_seasonality,
            area_seasonality,
        ],
        "Weekday_Store_Sales",
    )
    Weekday_Delivery_Sales = get_single_seasonality(
        [
            location_seasonality,
            industry_seasonality,
            product_seasonality,
            area_seasonality,
        ],
        "Weekday_Delivery_Sales",
    )
    Weekend_Store_Sales = get_single_seasonality(
        [
            location_seasonality,
            industry_seasonality,
            product_seasonality,
            area_seasonality,
        ],
        "Weekend_Store_Sales",
    )
    Weekend_Delivery_Sales = get_single_seasonality(
        [
            location_seasonality,
            industry_seasonality,
            product_seasonality,
            area_seasonality,
        ],
        "Weekend_Store_Sales",
    )
    return (
        Weekday_Store_Sales,
        Weekday_Delivery_Sales,
        Weekend_Store_Sales,
        Weekend_Delivery_Sales,
    )


def derived_fields(df: pd.DataFrame) -> pd.DataFrame:
    for j in [
        "Weekend_Delivery_Sales",
        "Weekend_Store_Sales",
        "Weekday_Delivery_Sales",
        "Weekday_Store_Sales",
    ]:
        df[j] = df[j].fillna(0)
    df["Weekday_Total_Sales"] = df["Weekday_Delivery_Sales"] + df["Weekday_Store_Sales"]
    df["Weekend_Total_Sales"] = df["Weekend_Delivery_Sales"] + df["Weekend_Store_Sales"]
    df["Monthly_Store_Sales"] = (
        df["Weekday_Store_Sales"] * 20 + df["Weekend_Store_Sales"] * 8
    )
    df["Monthly_Delivery_Sales"] = (
        df["Weekday_Delivery_Sales"] * 20 + df["Weekend_Delivery_Sales"] * 8
    )
    df["Monthly_Sales"] = df["Monthly_Store_Sales"] + df["Monthly_Delivery_Sales"]
    df["Delivery_%"] = df["Monthly_Delivery_Sales"] / df["Monthly_Sales"]
    return df


def forward_fill(records: list[dict]):
    records_tmp = [i for i in records if i["Monthly_Sales"]]
    sales_period = records_tmp[0]["Sales_Period"]
    records = [record for record in records if record["Sales_Period"] > sales_period]
    if len(records) == 0:
        return
    weekend_delivery_sales = records[0].get("Weekend_Delivery_Sales", None)
    weekday_delivery_sales = records[0].get("Weekday_Delivery_Sales", None)
    weekend_store_sales = records[0].get("Weekend_Store_Sales", None)
    weekday_store_sales = records[0].get("Weekday_Store_Sales", None)
    if (
        weekend_delivery_sales is None
        or weekday_delivery_sales is None
        or weekend_store_sales is None
        or weekday_store_sales is None
    ):
        return
    for i in records:
        if (
            i.get("Source", "") != "Generated"
            and i.get("Study", "") != "Generated"
            and i.get("Researcher", "") != "Mahmoud"
        ):
            weekend_delivery_sales = i.get(
                "Weekend_Delivery_Sales", weekend_delivery_sales
            )
            weekday_delivery_sales = i.get(
                "Weekday_Delivery_Sales", weekday_delivery_sales
            )
            weekend_store_sales = i.get("Weekend_Store_Sales", weekend_store_sales)
            weekday_store_sales = i.get("Weekday_Store_Sales", weekday_store_sales)
            continue
        year = i["Sales_Year"]
        month = i["Sales_Month"]
        location_type = i["Location_Type"]
        industry = i["Industry_Level_2"]
        product_focus = i["Product_Focus"]
        area = i["Level_3_Area"]
        (
            Weekday_Store_Sales_Seasonality,
            Weekday_Delivery_Sales_Seasonality,
            Weekend_Store_Sales_Seasonality,
            Weekend_Delivery_Sales_Seasonality,
        ) = get_seasonalities(year, month, location_type, industry, product_focus, area)

        if Weekday_Store_Sales_Seasonality != None:
            i["Weekday_Store_Sales"] = (
                weekday_store_sales
                + weekday_store_sales * Weekday_Store_Sales_Seasonality
            )
        if Weekday_Delivery_Sales_Seasonality != None:
            i["Weekday_Delivery_Sales"] = (
                weekday_delivery_sales
                + weekday_delivery_sales * Weekday_Delivery_Sales_Seasonality
            )
        if Weekend_Store_Sales_Seasonality != None:
            i["Weekend_Store_Sales"] = (
                weekend_store_sales
                + weekend_store_sales * Weekend_Store_Sales_Seasonality
            )
        if Weekend_Delivery_Sales_Seasonality != None:
            i["Weekend_Delivery_Sales"] = (
                weekend_delivery_sales
                + weekend_delivery_sales * Weekend_Delivery_Sales_Seasonality
            )
        weekday_store_sales = i["Weekday_Store_Sales"]
        weekday_delivery_sales = i["Weekday_Delivery_Sales"]
        weekend_store_sales = i["Weekend_Store_Sales"]
        weekend_delivery_sales = i["Weekend_Delivery_Sales"]
        location_type = i["Location_Type"]
        industry = i["Industry_Level_2"]
        product_focus = i["Product_Focus"]
        area = i["Level_3_Area"]
        new_sales_update_single_record(
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
        )
    print(
        weekday_store_sales,
        weekday_delivery_sales,
        weekend_store_sales,
        weekend_delivery_sales,
    )


def forecast(df: pd.DataFrame, key):
    if len(df[df[key].notna()]) == 0:
        return pd.DataFrame()

    try:
        m = prophet_forecast_model(df, key)
        forcast = get_prediction(m, 12)
    except:
        return pd.DataFrame()
    return forcast


def backwordfill(records: list[dict]):
    df = pd.DataFrame(records)
    df = df.to_dict(orient="records")
    i = list(reversed(df))[0]
    weekday_store_sales = i["Weekday_Store_Sales"]
    weekday_delivery_sales = i["Weekday_Delivery_Sales"]
    weekend_store_sales = i["Weekend_Store_Sales"]
    weekend_delivery_sales = i["Weekend_Delivery_Sales"]
    for i in reversed(df):
        if i["Weekday_Store_Sales"]:
            weekday_store_sales = i["Weekday_Store_Sales"]
            weekday_delivery_sales = i["Weekday_Delivery_Sales"]
            weekend_store_sales = i["Weekend_Store_Sales"]
            weekend_delivery_sales = i["Weekend_Delivery_Sales"]
            continue

        location_type = i["Location_Type"]
        industry = i["Industry_Level_2"]
        product_focus = i["Product_Focus"]
        area = i["Level_3_Area"]
        year = i["Sales_Year"]
        month = i["Sales_Month"]
        (
            Weekday_Store_Sales_Seasonality,
            Weekday_Delivery_Sales_Seasonality,
            Weekend_Store_Sales_Seasonality,
            Weekend_Delivery_Sales_Seasonality,
        ) = get_seasonalities(year, month, location_type, industry, product_focus, area)

        if Weekday_Store_Sales_Seasonality != None:
            weekday_store_sales = weekday_store_sales + weekday_store_sales * (
                Weekday_Store_Sales_Seasonality * -1
            )
        if Weekday_Delivery_Sales_Seasonality != None:
            weekday_delivery_sales = weekday_delivery_sales + weekday_delivery_sales * (
                Weekday_Delivery_Sales_Seasonality * -1
            )
        if Weekend_Store_Sales_Seasonality != None:
            weekend_store_sales = weekend_store_sales + weekend_store_sales * (
                Weekend_Store_Sales_Seasonality * -1
            )
        if Weekend_Delivery_Sales_Seasonality != None:
            weekend_delivery_sales = weekend_delivery_sales + weekend_delivery_sales * (
                Weekend_Delivery_Sales_Seasonality
            )
        print(
            weekday_store_sales,
            weekday_delivery_sales,
            weekend_store_sales,
            weekend_delivery_sales,
        )
        # new_sales_update_single_record(
        #     i["Reference_Full_ID"],
        #     year,
        #     month,
        #     location_type,
        #     industry,
        #     product_focus,
        #     area,
        #     weekday_store_sales,
        #     weekday_delivery_sales,
        #     weekend_store_sales,
        #     weekend_delivery_sales,
        # )


# def get_data_from_forecast(forcast, year, month):
#     v = forcast[forcast["ds"].dt.strftime("%Y-%-m") == f"{year}-{month}"]["yhat"]
#     return v.values[0]


def find_seg(data: list[dict]):
    start_date = data[0]["Sales_Period"]
    end_date = data[-1]["Sales_Period"]
    for i in data:
        monthly_sales = i["Monthly_Sales"]
        if type(monthly_sales) == int or type(monthly_sales) == float:
            start_date = i["Sales_Period"]
            break
    for i in reversed(data):
        monthly_sales = i["Monthly_Sales"]
        if type(monthly_sales) == int or type(monthly_sales) == float:
            end_date = i["Sales_Period"]
            break
    num_months = (end_date.year - start_date.year) * 12 + (
        end_date.month - start_date.month
    )

    result = [
        record for record in data if start_date <= record["Sales_Period"] <= end_date
    ]
    empty_months = 0
    for i in result:
        if i["Monthly_Sales"] == None:
            empty_months += 1
    if empty_months == 0:
        return None
    if num_months - empty_months > 6:
        return None
    return result


def fill_gaps():
    keys = [
        "Weekday_Store_Sales",
        "Weekday_Delivery_Sales",
        "Weekend_Store_Sales",
        "Weekend_Delivery_Sales",
    ]
    ids = list(new_sales_refenrece_ids_with_sales_count())
    count = 0
    for i in ids:
        sales = list(
            new_sales_collection.find(
                {
                    "Reference_Full_ID": i["_id"],
                }
            ).sort("Sales_Period")
        )
        sales = find_seg(sales)
        if not sales:
            continue
        #
        for i in sales:
            if i["Monthly_Sales"] == None:
                count += 1
        #
        df = pd.DataFrame(sales)
        for j in keys:
            print(i["Reference_Full_ID"], j)
            df[j] = df[j].interpolate()
        df = derived_fields(df)
        df = df.to_dict(orient="records")
        for i in df:
            weekday_store_sales = i["Weekday_Store_Sales"]
            weekday_delivery_sales = i["Weekday_Delivery_Sales"]
            weekend_store_sales = i["Weekend_Store_Sales"]
            weekend_delivery_sales = i["Weekend_Delivery_Sales"]
            location_type = i["Location_Type"]
            industry = i["Industry_Level_2"]
            product_focus = i["Product_Focus"]
            area = i["Level_3_Area"]
            year = i["Sales_Year"]
            month = i["Sales_Month"]
            new_sales_update_single_record(
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
            )


if __name__ == "__main__":
    """
    Loop through all the ids, find all close sales, with the same industry and location type with x distance
    """
    # setup_sales()
    fill_gaps()
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
