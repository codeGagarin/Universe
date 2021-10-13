from dataclasses import dataclass, replace
from datetime import date

from ...report_classes import ISReport
from ...period_utils import Period
from ...report_sender import ReportSender
from .details import details


@dataclass
class FrameTypes:
    PERIOD_TYPE: Period.Type
    DELTA: int


class ServiceBoard(ISReport):
    @classmethod
    def anchor_path(cls):
        return __file__

    @dataclass
    class Params:
        TAG: str or None
        CAPTION: str or None
        REPORT_DATE: date
        FRAME_NAME: str  # ServiceBoard.FrameNames
        SERVICE_FILTER: list or None
        EXECUTORS: list or None

    @dataclass
    class Locals:
        FROM = None
        TO = None
        IS_WEEK = None
        IS_MONTH = None
        IS_QTR = None
        IS_CURRENT = None
        CURRENT_QTR = 'current_qtr'
        LAST_QTR = 'last_qtr'
        CURRENT_WEEK = 'current_week'
        LAST_WEEK = 'last_week'
        CURRENT_MONTH = 'current_month'
        LAST_MONTH = 'last_month'
        UTL_PERIODS = None

    @dataclass
    class PresetTypes:
        BA_OAK = 'РРТ Бизнес Анализ'
        STATION = 'Станция ФУЛ'
        PROSTO12 = 'Легкие решения'
        INTRATOOL = 'Интратул ФУЛ'
        BGL = 'БиДжи 2-я линия'
        PRJ_OAK_01 = 'ОАК Инфраструткура'
        MINVODY = 'Минеральные воды'

    _presets = {
        PresetTypes.BA_OAK: Params(
            TAG='Группа БА',
            CAPTION='Отчет по проектам и утилизации',
            FRAME_NAME=Locals.LAST_WEEK,
            SERVICE_FILTER=[188],
            REPORT_DATE=date.today(),
            EXECUTORS=[9377, 9380, 9758]
        ),
        PresetTypes.STATION: Params(
            TAG='УК Стация',
            CAPTION='Отчет по сервисам и утилизация',
            FRAME_NAME=Locals.LAST_WEEK,
            SERVICE_FILTER=[139],
            REPORT_DATE=date.today(),
            EXECUTORS=[7162, 9131, 9070]
        ),
        PresetTypes.PROSTO12: Params(
            TAG='Легкие решения',
            CAPTION='Утилизация по всем сервисам',
            FRAME_NAME=Locals.LAST_WEEK,
            SERVICE_FILTER=None,
            REPORT_DATE=date.today(),
            EXECUTORS=[396, 5994, 405, 7154, 5995, 390, 43]
        ),
        PresetTypes.INTRATOOL: Params(
           TAG='Интратул',
           CAPTION='Сервисы и утилизация',
           FRAME_NAME=Locals.LAST_WEEK,
           SERVICE_FILTER=[66, 118, 134, 136, 137, 169],
           REPORT_DATE=date.today(),
           EXECUTORS=[7379, 5329, 7988, 396, 5731, 8882, 7154, 5372, 5994, 5912, 8958, 8949, 9626]
        ),
        PresetTypes.BGL: Params(
            TAG='БиДжи',
            CAPTION='Сервис отчет',
            FRAME_NAME=Locals.LAST_WEEK,
            SERVICE_FILTER=[198],
            REPORT_DATE=date.today(),
            EXECUTORS=None
        ),
        PresetTypes.PRJ_OAK_01: Params(
            TAG='OAK',
            CAPTION='Оптимизация инраструктуры',
            FRAME_NAME=Locals.LAST_WEEK,
            SERVICE_FILTER=[200],
            REPORT_DATE=date.today(),
            EXECUTORS=None
        ),
        PresetTypes.MINVODY: Params(
            TAG='Минеральные воды',
            CAPTION='Задачи ИТ',
            FRAME_NAME=Locals.LAST_WEEK,
            SERVICE_FILTER=[201],
            REPORT_DATE=date.today(),
            EXECUTORS=[9632]
        ),
    }

    def update_locals(self, _params, _locals) -> None:
        _locals.FRAMES = {
            _locals.LAST_QTR: FrameTypes(
                PERIOD_TYPE=Period.Type.QTR,
                DELTA=-1
            ),
            _locals.CURRENT_QTR: FrameTypes(
                PERIOD_TYPE=Period.Type.QTR,
                DELTA=0
            ),
            _locals.LAST_MONTH: FrameTypes(
                PERIOD_TYPE=Period.Type.MONTH,
                DELTA=-1
            ),
            _locals.CURRENT_MONTH: FrameTypes(
                PERIOD_TYPE=Period.Type.MONTH,
                DELTA=0
            ),
            _locals.LAST_WEEK: FrameTypes(
                PERIOD_TYPE=Period.Type.WEEK,
                DELTA=-1
            ),
            _locals.CURRENT_WEEK: FrameTypes(
                PERIOD_TYPE=Period.Type.WEEK,
                DELTA=0
            ),
        }

        current_frame_type = \
            _locals.FRAMES[_params.FRAME_NAME].PERIOD_TYPE

        report_period = Period(
            _params.REPORT_DATE,
            current_frame_type,
            _locals.FRAMES[self._params.FRAME_NAME].DELTA,
        )

        _locals.FROM = report_period.begin
        _locals.TO = report_period.end

        _locals.IS_WEEK = True if current_frame_type == Period.Type.WEEK else False
        _locals.IS_MONTH = True if current_frame_type == Period.Type.MONTH else False
        _locals.IS_QTR = True if current_frame_type == Period.Type.QTR else False

        _locals.IS_CURRENT = _params.FRAME_NAME in (
            _locals.CURRENT_WEEK,
            _locals.CURRENT_MONTH,
            _locals.CURRENT_QTR,
        )

        period_map = (
            ('mo tu we th fr sa su', Period.Type.DAY, True),
            ('wh1 wh2 wh3', Period.Type.WEEK, False),  # week history
            ('mh1 mh2 mh3', Period.Type.MONTH, False),  # month history
            ('qh1 qh2 qh3', Period.Type.QTR, False),  # qtr history
        )

        _locals.UTL_PERIODS = {'ttl': report_period}

        for names, _type, time_forward in period_map:
            for deep, name in enumerate(names.split(), start=0 if time_forward else 1):
                _locals.UTL_PERIODS[name] = Period(
                    report_period.begin,
                    _type,
                    (1 if time_forward else -1) * deep
                )

    def update_navigation(self, _params, _locals, _data) -> None:
        for name in _locals.FRAMES.keys():
            self.add_nav_point(
                name,
                replace(_params, FRAME_NAME=name)
            )

    def update_data(self, _params, _locals, _data) -> None:
        _data['srv'] = []  # default result
        _data['utl'] = []  # default result

        if _params.SERVICE_FILTER:
            with self.cursor(named=True) as cursor:
                cursor.execute(
                    self.load_query(
                        query_path='query_services.sql',
                        params={
                            'service_list': _params.SERVICE_FILTER,
                            'from': _locals.FROM,
                            'to': _locals.TO,
                        }
                    ),
                )
                _data['srv'] = cursor.fetchall()

        if _params.EXECUTORS:
            with self.cursor() as cursor:
                cursor.execute(
                    self.load_query(
                        query_path='query_utilization.sql',
                        params={
                            'from': _locals.FROM,
                            'to': _locals.TO,
                            'service_list': _params.SERVICE_FILTER,
                            'user_list': _params.EXECUTORS,
                            'frame_type': _locals.FRAMES[_params.FRAME_NAME].PERIOD_TYPE,
                            'PeriodType': Period.Type,
                        }
                    ),
                )
                _data['utl'] = cursor.fetchall()

    def update_details(self, _params, _locals, _data) -> None:
        for kind, detail in details(self).items():
            for key in detail.KEYS:
                self.add_detail(
                    key,
                    detail.CHANGER(key, detail.PARAMS),
                    kind=kind)

    @staticmethod
    def pretty_caption(preset_name):
        return '[{}]{}'.format(
            ServiceBoard.presets()[preset_name].TAG,
            ServiceBoard.presets()[preset_name].CAPTION,
        )


class ServiceBoardSender(ReportSender):
    REPORT_TYPE = ServiceBoard
    MAIL_LIST = (
        # ReportSender.MailerParams(
        #     PRESET_NAME=ServiceBoard.PresetTypes.BA_OAK,
        #     SMTP='RRT',
        #     TO=ServiceBoard.presets()[ServiceBoard.PresetTypes.BA_OAK].EXECUTORS,
        #     CC=['k.kondrashevich@rrt.ru', 'igor.belov@rrt.ru'],
        #     SUBJECT=ServiceBoard.pretty_caption(ServiceBoard.PresetTypes.BA_OAK),
        # ),
        ReportSender.MailerParams(
            PRESET_NAME=ServiceBoard.PresetTypes.STATION,
            SMTP='STH',
            TO=ServiceBoard.presets()[ServiceBoard.PresetTypes.STATION].EXECUTORS,
            CC=['alexey.makarov@station-hotels.ru', 'igor.belov@station-hotels.ru'],
            SUBJECT=ServiceBoard.pretty_caption(ServiceBoard.PresetTypes.STATION),
        ),
        ReportSender.MailerParams(
            PRESET_NAME=ServiceBoard.PresetTypes.PROSTO12,
            SMTP='P12',
            TO=ServiceBoard.presets()[ServiceBoard.PresetTypes.PROSTO12].EXECUTORS,
            CC=['v.ulianov@prosto12.ru', 'a.zhuk@prosto12.ru', 'i.belov@prosto12.ru'],
            SUBJECT=ServiceBoard.pretty_caption(ServiceBoard.PresetTypes.PROSTO12),
        ),
        ReportSender.MailerParams(
            PRESET_NAME=ServiceBoard.PresetTypes.INTRATOOL,
            SMTP='NTR',
            TO=ServiceBoard.presets()[ServiceBoard.PresetTypes.PROSTO12].EXECUTORS,
            CC=[8827, 'v.ulianov@prosto12.ru', 'a.zhuk@prosto12.ru', 'i.belov@prosto12.ru'],
            SUBJECT=ServiceBoard.pretty_caption(ServiceBoard.PresetTypes.INTRATOOL),
        ),
        ReportSender.MailerParams(
            PRESET_NAME=ServiceBoard.PresetTypes.BGL,
            SMTP='P12',
            TO=['it@bglogistic.ru'],
            CC=['v.ulianov@prosto12.ru', 'i.belov@prosto12.ru'],
            SUBJECT=ServiceBoard.pretty_caption(ServiceBoard.PresetTypes.BGL),
        ),
        ReportSender.MailerParams(
            PRESET_NAME=ServiceBoard.PresetTypes.PRJ_OAK_01,
            SMTP='P12',
            TO=['v.ulianov@prosto12.ru', 'i.belov@prosto12.ru'],
            CC=None,
            SUBJECT=ServiceBoard.pretty_caption(ServiceBoard.PresetTypes.PRJ_OAK_01),
        ),
        # ReportSender.MailerParams(
        #     PRESET_NAME=ServiceBoard.PresetTypes.MINVODY,
        #     SMTP='STH',
        #     TO=ServiceBoard.presets()[ServiceBoard.PresetTypes.MINVODY].EXECUTORS,
        #     CC=['alsep975@gmail.com', 'olgaavalishvili@minvody.net', 'igor.belov@station-hotels.ru'],
        #     SUBJECT=ServiceBoard.pretty_caption(ServiceBoard.PresetTypes.MINVODY),
        # ),
    )

    @classmethod
    def get_crontab(cls):
        return '0 7 * * 1'
