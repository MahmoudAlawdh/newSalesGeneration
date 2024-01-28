import pandas as __pd

from db.helpers import new_sales_collection as __new_sales_collection
from db.queries import (
    new_sales_refenrece_ids_with_sales_count as __new_sales_refenrece_ids_with_sales_count,
)
from db.queries import (
    new_sales_update_single_record as __new_sales_update_single_record,
)
from helpers.sales import derived_fields as __derived_fields


def __find_seg(data: list[dict], seg_len: int):
    seg = []
    for i in data:
        if i["Monthly_Sales"] == None and len(seg) > 0:
            seg.append(i)

        if i["Monthly_Sales"] != None:
            seg.append(i)
            if len(seg) > seg_len:
                if seg[0]["Monthly_Sales"] != None and seg[-1]["Monthly_Sales"] != None:
                    yield seg
                    seg = []
                else:
                    seg = [i]


def fill_gaps():
    keys = [
        "Weekday_Store_Sales",
        "Weekday_Delivery_Sales",
        "Weekend_Store_Sales",
        "Weekend_Delivery_Sales",
    ]
    ids = list(__new_sales_refenrece_ids_with_sales_count())
    count = 0
    for i in ids:
        sales = list(
            __new_sales_collection.find(
                {
                    "Reference_Full_ID": i["_id"],
                }
            ).sort("Sales_Period")
        )
        sales = __find_seg(sales, 3)
        if not sales:
            continue
        #
        for j in sales:
            for i in j:
                if i["Monthly_Sales"] == None:
                    count += 1
            if count == 0:
                continue
            df = __pd.DataFrame(j)
            try:
                for j in keys:
                    df[j] = df[j].interpolate(limit_area="inside")
                df = __derived_fields(df)
                df = df.to_dict(orient="records")
                print(i["Reference_Full_ID"], count)
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
                    __new_sales_update_single_record(
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
            except:
                print("Error")
