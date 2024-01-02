from db.helpers import new_sales_collection
from db.queries import new_sales_average_sale
from helpers.Sales import (
    clear_db,
    copy_sales,
    fill_sales_gaps,
    generate_all_sales_records,
    get_sales_primary_id_and_reference_ids_set,
)


def setup_sales():
    clear_db()
    copy_sales()
    fill_sales_gaps()
    generate_all_sales_records()


if __name__ == "__main__":
    """
    Loop through all the ids, find all close sales, with the same industry and location type with x distance
    """
    data = list(new_sales_average_sale())
    count = 0
    c = 0
    datalen = len(data)
    for i in data:
        print(c, datalen)
        c += 1
        sales = list(
            new_sales_collection.find(
                {
                    "Level_1_Area": "Kuwait",
                    "Level_3_Area": i["area"],
                    # "Industry_Level_2": i["industry"],
                    "Sales_Month": i["month"],
                    "Sales_Year": i["year"],
                    "location_type": i["location_type"],
                }
            )
        )
        if len(sales) > 0:
            item = [item for item in sales if item["Monthly_Sales"] != 0]
            if item[0]:
                print(item[0]["Monthly_Sales"], i["average_sales"])
            count += len(sales)
            print(f"recordsFound {count}")
        # selected_items = [item for item in data if item['Sales_Year'] i]
        # point = i.get("point")
        # if not point:
        #     continue
        # radius_in_km = 0.500
        # radius_in_radians = radius_in_km / 6371
        # sales = new_sales_collection.aggregate(
        #     [
        #         {
        #             "$match": {
        #                 "Reference_Full_ID": {"$ne": i.get("Reference_Full_ID")},
        #                 "Sales_Month": i.get("Sales_Month"),
        #                 "Sales_Year": i.get("Sales_Year"),
        #                 "Monthly_Sales": {"$ne": None},
        #                 "Industry_Level_2": i.get("Industry_Level_2"),
        #                 "Level_3_Area": i.get("Level_3_Area"),
        #                 # "point": {
        #                 #     "$geoWithin": {
        #                 #         "$centerSphere": [
        #                 #             point.get("coordinates"),
        #                 #             radius_in_radians,
        #                 #         ]
        #                 #     }
        #                 # },
        #             }
        #         },
        #         {"$limit": 1000},
        #     ]
        # )
        # if count == 1000:
        #     break
    print(count)
