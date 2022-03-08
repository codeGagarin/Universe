from datetime import datetime, date, timedelta
from dataclasses import dataclass


def _dt_min(d: date) -> datetime:
    return datetime.combine(d, datetime.min.time())


def _dt_max(d: date) -> datetime:
    return datetime.combine(d, datetime.max.time())


def get_period(now: date, period_type: str, delta: int, with_time=False) -> dict:
    """
    Возвращает словарь с параметрами периода
    :param now: текущая дата
    :param period_type: тип периода day, week, month, qtr
    :param delta: сколько периодов отнять или прибавить
    :param with_time: -- 'from' and 'to' is datetime type
    :return: параметры периода:
        'from' -- дата начала
        'to' -- дата окончания
        'id' -- идентификатор периода 1945 (45 неделя 19-го года)
        'type' -- тип периода (см: period_type)

    >>> get_period(date(2021, 3, 9), 'qtr', -1)
    {'from': datetime.date(2020, 10, 1), 'to': datetime.date(2020, 12, 31), 'type': 'qtr', 'id': '4q20'}

    """
    result = {'from': None, 'to': None, 'type': period_type, 'id': None}
    if period_type is 'month':
        y = now.year
        m = now.month
        sign = -1 if delta < 0 else 1
        delta_y = (delta * sign // 12) * sign
        delta_m = (delta * sign % 12) * sign
        y += delta_y
        m += delta_m
        if m > 12:
            y += 1
            m -= 12
        elif m < 1:
            y -= 1
            m += 12
        result['from'] = date(y, m, 1)
        y, m = (y, m + 1) if m != 12 else (y + 1, 1)
        next_month = date(y, m, 1)
        result['to'] = next_month - timedelta(days=1)
        result['id'] = result['from'].strftime('%-mm%y')
    elif period_type is 'week':
        result['from'] = now + timedelta(days=-now.isoweekday() + 1, weeks=delta)
        result['to'] = result['from'] + timedelta(days=6)
        result['id'] = result['from'].strftime('%-Ww%y')
    elif period_type is 'day':
        result['from'] = now + timedelta(days=delta)
        result['to'] = result['from']
        result['id'] = result['from'].strftime('%m%d')
    elif period_type is 'qtr':
        qtr_num = (now.month-1)//3 + 1
        qtr_first_month = (qtr_num-1)*3+1
        result['from'] = get_period(date(now.year, qtr_first_month, 1), 'month', delta*3 )['from']
        result['to'] = get_period(result['from'], 'month', 2)['to']
        new_qtr_num = (result['from'].month - 1) // 3 + 1
        result['id'] = result['to'].strftime(f'{new_qtr_num}q%y')
    if with_time:
        result['from'] = _dt_min(result['from'])
        result['to'] = _dt_max(result['to'])
    return result


@dataclass
class Period:
    @dataclass
    class Types:
        DAY = 'day'
        WEEK = 'week'
        MONTH = 'month'
        QTR = 'qtr'

    # TODO: deprecated, backward compatibility
    Type = Types

    begin: datetime
    end: datetime
    id: str
    period_type: Type

    def __init__(self, now: date = None, period_type: Types = Types.MONTH, delta: int = 0):
        now = now or datetime.now()
        period_data = get_period(now, str(period_type), delta)
        self.begin = period_data['from']
        self.end = period_data['to']
        self.id = period_data['id']
        self.period_type = period_type

