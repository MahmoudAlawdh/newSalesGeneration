from typing import Literal

from pymongo.write_concern import WriteConcern

from .connection import _client


def __get_db(
    database: Literal["gm", "NewSales"], collection: Literal["Sales", "Stores"]
):
    return _client[database][collection].with_options(write_concern=WriteConcern(w=0))


gm_sales_collection = __get_db("gm", "Sales")
gm_stores_collection = __get_db("gm", "Stores")
new_sales_collection = __get_db("NewSales", "Sales")
