from datetime import datetime , timezone
from typing import Text

from dateutil.relativedelta import relativedelta

def first_day_of_month(offset_month:int = 0 , tzinfo: timezone = timezone.utc):
    """
    Return the timestamp representing the fitrst day of month


    :param offset_month: offset added to current month
    :param tzinfo:
    :return: return first day of month
    """
    d = datetime.utcnow().replace(day =1 ,minute=0 , second=0,microsecond=0,tzinfo=tzinfo)
    delta = relativedelta(month=offset_month)
    target_date= d +delta
    return target_date


def year_month_day(s : Text) -> datetime:
    if s != '':
        date = datetime.fromisoformat(f'{s}T00:00:00+00:00')
    else:
        date = None
    return date

def current_date_yyymmdd() -> int:
    """
    Returns:
        str : this function returns current_date in YYYMMMDD format
    """
    return int(datetime.utcnow().strftime("%Y%m%d"))







