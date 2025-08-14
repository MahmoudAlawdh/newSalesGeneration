from datetime import datetime as _datetime

from dateutil.relativedelta import relativedelta
from monthdelta import monthdelta as _monthdelta


def get_last_day_of_the_month(date: _datetime):
    last_day = date.replace(day=28) + relativedelta(days=4)  # this will never fail
    return last_day - relativedelta(days=last_day.day)


def __get_list_of_sales_period(opening_date: _datetime, closing_date: _datetime):
    curr_date = opening_date
    while curr_date <= closing_date:
        last_day = get_last_day_of_the_month(curr_date)
        yield last_day
        curr_date += _monthdelta(1)
