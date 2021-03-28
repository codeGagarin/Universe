from dataclasses import replace

from ...report_classes import Report
from ..task_report import TaskReport
from ..expenses_report import ExpensesReport


def details(report):
    get_details_func = (
        details_srv,
        details_utl,
        details_utl_evaluate
    )
    result = {}
    [result.update(func(report)) for func in get_details_func]
    return result


def changes_hook_utl(key, template):
    return replace(template, USER_FILTER=(key,))


def details_utl_evaluate(report):
    _params = report.get_params()
    _locals = report.get_locals()
    _data = report.get_data()
    utl_keys = [rec.id for rec in _data['utl']]
    task_report_params = TaskReport.Params(
        CREATED_FROM=None,
        CREATED_TO=None,
        CLOSED_IS_NULL=None,
        CLOSED_FROM=_locals.FROM,
        CLOSED_TO=_locals.TO,
        SERVICE_FILTER=_params.SERVICE_FILTER,
        USER_FILTER=None,
        FIELD_LIST=(
            TaskReport.Fields.CREATOR,
            TaskReport.Fields.CREATED,
            TaskReport.Fields.CLOSED,
            TaskReport.Fields.EVALUATION,
            TaskReport.Fields.SERVICE
        ),
        NO_EXEC=None,
        PLANED_IS_NULL=None,
        EVALUATION_FILTER=None,
    )

    return {
        name: Report.Details(
            KEYS=utl_keys,
            PARAMS=replace(
                task_report_params,
                EVALUATION_FILTER=(value,),
            ),
            CHANGER=changes_hook_utl
        ) for name, value in {
            f'ev{v}': v for v in range(1, 6)
        }.items()
    }


def details_utl(report):
    _params = report.get_params()
    _locals = report.get_locals()
    _data = report.get_data()

    utl_keys = [rec.id for rec in _data['utl']]

    task_report_params = TaskReport.Params(
        CREATED_FROM=None,
        CREATED_TO=None,
        CLOSED_IS_NULL=None,
        CLOSED_TO=None,
        CLOSED_FROM=None,
        SERVICE_FILTER=_params.SERVICE_FILTER,
        USER_FILTER=None,
        FIELD_LIST=(
            TaskReport.Fields.CREATOR,
            TaskReport.Fields.CREATED,
            TaskReport.Fields.CLOSED,
            TaskReport.Fields.EVALUATION,
            TaskReport.Fields.SERVICE
        ),
        NO_EXEC=None,
        PLANED_IS_NULL=None,
        EVALUATION_FILTER=None,
    )
    expenses_report_params = ExpensesReport.Params(
        CLOSED_IS_NULL=None,
        CLOSED_FROM=None,
        CLOSED_TO=None,
        DATE_EXP_FROM=None,
        DATE_EXP_TO=None,
        SERVICE_FILTER=_params.SERVICE_FILTER,
        USER_FILTER=None,
        FIELD_LIST=(
            ExpensesReport.Fields.CREATOR,
            ExpensesReport.Fields.CREATED,
            ExpensesReport.Fields.CLOSED,
            ExpensesReport.Fields.SERVICE

        )
    )

    result = {
        'own_task': Report.Details(
            KEYS=utl_keys,
            PARAMS=replace(
                task_report_params,
                CLOSED_IS_NULL=True,
            ),
            CHANGER=changes_hook_utl
        )
    }

    for name, period in _locals.UTL_PERIODS.items():
        result[f'{name}t'] = Report.Details(
            KEYS=utl_keys,
            PARAMS=replace(
                task_report_params,
                CLOSED_FROM=period.begin,
                CLOSED_TO=period.end,
            ),
            CHANGER=changes_hook_utl
        )
        result[f'{name}u'] = Report.Details(
            KEYS=utl_keys,
            PARAMS=replace(
                expenses_report_params,
                DATE_EXP_FROM=period.begin,
                DATE_EXP_TO=period.end,
            ),
            CHANGER=changes_hook_utl
        )

    return result


def details_srv(report):
    _locals = report.get_locals()
    _data = report.get_data()

    def changes_hook_srv(key, template):
        return replace(template, SERVICE_FILTER=(key,))

    srv_keys = [rec.id for rec in _data['srv']]

    task_report_params = TaskReport.Params(
        CREATED_FROM=None,
        CREATED_TO=None,
        CLOSED_IS_NULL=None,
        CLOSED_TO=None,
        CLOSED_FROM=None,
        SERVICE_FILTER=None,
        USER_FILTER=None,
        FIELD_LIST=(
            TaskReport.Fields.CREATOR,
            TaskReport.Fields.CREATED,
            TaskReport.Fields.CLOSED,
            TaskReport.Fields.PLANED,
        ),
        NO_EXEC=None,
        PLANED_IS_NULL=None,
        EVALUATION_FILTER=None,
    )
    expenses_report_params = ExpensesReport.Params(
        CLOSED_IS_NULL=None,
        CLOSED_FROM=None,
        CLOSED_TO=None,
        DATE_EXP_FROM=None,
        DATE_EXP_TO=None,
        SERVICE_FILTER=None,
        USER_FILTER=None,
        FIELD_LIST=(
            ExpensesReport.Fields.CREATOR,
            ExpensesReport.Fields.CREATED,
            ExpensesReport.Fields.CLOSED,
            ExpensesReport.Fields.EXECUTOR,
        )
    )

    return {
        'created': Report.Details(
            KEYS=srv_keys,
            PARAMS=replace(
                task_report_params,
                CREATED_FROM=_locals.FROM,
                CREATED_TO=_locals.TO,
            ),
            CHANGER=changes_hook_srv,
        ),
        'closed': Report.Details(
            KEYS=srv_keys,
            PARAMS=replace(
                task_report_params,
                CLOSED_FROM=_locals.FROM,
                CLOSED_TO=_locals.TO,
            ),
            CHANGER=changes_hook_srv
        ),
        'open': Report.Details(
            KEYS=srv_keys,
            PARAMS=replace(
                task_report_params,
                CLOSED_IS_NULL=True,
            ),
            CHANGER=changes_hook_srv
        ),
        'no_exec': Report.Details(
            KEYS=srv_keys,
            PARAMS=replace(
                task_report_params,
                CLOSED_IS_NULL=True,
                NO_EXEC=True,
            ),
            CHANGER=changes_hook_srv
        ),
        'no_planed': Report.Details(
            KEYS=srv_keys,
            PARAMS=replace(
                task_report_params,
                CLOSED_IS_NULL=True,
                PLANED_IS_NULL=True,
            ),
            CHANGER=changes_hook_srv
        ),
        'closed_exp': Report.Details(
            KEYS=srv_keys,
            PARAMS=replace(
                expenses_report_params,
                CLOSED_FROM=_locals.FROM,
                CLOSED_TO=_locals.TO,
            ),
            CHANGER=changes_hook_srv
        ),
        'open_exp': Report.Details(
            KEYS=srv_keys,
            PARAMS=replace(
                expenses_report_params,
                CLOSED_IS_NULL=True,
            ),
            CHANGER=changes_hook_srv
        ),

    }
