from typing import Literal

from helpers.sales import clear_db as __clear_db
from helpers.sales import copy_sales as __copy_sales
from helpers.sales import fill_sales_gaps as __fill_sales_gaps


def setup_sales(country: list[Literal["Kuwait", "Bahrain", "Qatar"]]):
    __clear_db()
    __copy_sales(country)
    __fill_sales_gaps(country)
