import json

import numpy as __np
import pandas as __pd

from db.helpers import new_sales_collection as __new_sales_collection
from helpers.prophet_helper import get_prediction, prophet_forecast_model


def prophet_forcast(brands: list[str]):
    query = __new_sales_collection.aggregate(
        [
            {
                "$match": {
                    "Brand": {"$in": brands},
                }
            },
            {"$group": {"_id": "$Reference_Full_ID", "fieldN": {"$push": "$$ROOT"}}},
        ]
    )
    keys = [
        "Weekday_Store_Sales",
        "Weekday_Delivery_Sales",
        "Weekend_Store_Sales",
        "Weekend_Delivery_Sales",
    ]
    for i in query:
        df = __pd.DataFrame(i["fieldN"])
        first = df.iloc[0]
        try:
            for j in keys:
                print(first["Reference_Full_ID"], len(df))
                model = prophet_forecast_model(df, j)
                prediction = get_prediction(model, 36)
                for idx, row in prediction.iterrows():
                    __new_sales_collection.update_one(
                        {
                            "Reference_Full_ID": first["Reference_Full_ID"],
                            "Sales_Month": row["ds"].month,
                            "Sales_Year": row["ds"].year,
                            f"{j}": None,
                            "original": False,
                        },
                        {"$set": {f"{j}": round(row["yhat"])}},
                    )
        except:
            pass
