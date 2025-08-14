from dataclasses import dataclass
from typing import Optional

from helpers.types import CountryList


@dataclass
class Config:
    countries: CountryList
    sales_start: int
    sales_end: Optional[int]
    brands: Optional[list[str]] = None
