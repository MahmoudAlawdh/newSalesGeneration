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

    def check_values_is_None(record: dict):
        return (
            record.get("Weekday_Delivery_Sales", None) == None
            or record.get("Weekday_Store_Sales") == None
            or record.get("Weekend_Delivery_Sales", None) == None
            or record.get("Weekend_Store_Sales", None) == None
        )

    def check_values_not_None(record: dict):
        return (
            record.get("Weekday_Delivery_Sales", None) != None
            or record.get("Weekday_Store_Sales", None) != None
            or record.get("Weekend_Delivery_Sales", None) != None
            or record.get("Weekend_Store_Sales", None) != None
        )

    for i in data:
        is_none = check_values_is_None(i)
        not_none = check_values_not_None(i)
        if is_none and len(seg) > 0:
            seg.append(i)

        if not_none:
            seg.append(i)
            if len(seg) > seg_len:
                if check_values_not_None(seg[0]) and check_values_not_None(seg[-1]):
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
        sales = list(__find_seg(sales, 3))
        if not sales:
            continue
        for j in sales:
            for i in j:
                if (
                    i.get("Weekday_Delivery_Sales") == None
                    or i.get("Weekday_Store_Sales", None) == None
                    or i.get("Weekend_Delivery_Sales", None) == None
                    or i.get("Weekend_Store_Sales", None) == None
                ):
                    count += 1
            df = __pd.DataFrame(j)
            for j in keys:
                if j not in df:
                    continue
                df[j] = df[j].interpolate(limit_area="inside")
            df = df.to_dict(orient="records")
            for i in df:
                try:
                    weekday_store_sales = i.get("Weekday_Store_Sales", None)
                    weekday_delivery_sales = i.get("Weekday_Delivery_Sales", None)
                    weekend_store_sales = i.get("Weekend_Store_Sales", None)
                    weekend_delivery_sales = i.get("Weekend_Delivery_Sales", None)
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
                except Exception as error:
                    print("Error", error)
