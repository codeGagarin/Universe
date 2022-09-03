import json
from datetime import datetime
from psycopg2 import connect, sql
from psycopg2.extensions import connection

from keys import KeyChain
import common

session: connection


def create_table():
    with open('trace_create.sql') as query_file:
        with session.cursor() as cursor:
            cursor.execute(
                query_file.read().format(
                    table_name=common.TABLE_FOR_REPORT_STORING,
                    lambda_group=common.LAMBDA_GROUP
                )
            )
    session.commit()


def _old_zone_calc(current_report_id, zone_current_ip):
    """ Return None if zone ip not changed, else prev zone IP """
    with session.cursor() as cursor:
        cursor.execute(
            sql.SQL("""
                SELECT report_body->>'zone_ip' FROM {table_name}
                WHERE report_id<{report_id} ORDER BY report_id LIMIT 1 
            """).format(
                table_name=sql.Identifier(
                    common.TABLE_FOR_REPORT_STORING
                ),
                report_id=sql.Literal(current_report_id)
            )
        )
        result = cursor.fetchone()

        if not result:
            return None  # first report

        zone_prev_ip = result[0]

        return None if zone_current_ip == zone_prev_ip else zone_prev_ip


def submit_report(report_body):
    json_body = json.loads(report_body)
    report_id = datetime.now()
    with session.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                'INSERT INTO {table_name} (report_id, is_ok, is_diff, old_zone, report_body)'
                'VALUES ({report_id}, {is_ok}, {is_diff}, {old_zone}, {report_body})'
            ).format(
                table_name=sql.Identifier(
                    common.TABLE_FOR_REPORT_STORING
                ),
                report_id=sql.Literal(
                    report_id
                ),
                is_ok=sql.Literal(
                    json_body['is_ok']
                ),
                is_diff=sql.Literal(
                    bool(json_body['diff'])
                ),
                old_zone=sql.Literal(
                    _old_zone_calc(
                        report_id, json_body['zone_ip']
                    )
                ),
                report_body=sql.Literal(report_body)
            )
        )
    session.commit()


def main(event, context):
    global session
    session = connect(**KeyChain.PG_STH_DNS_SYNC)
    test_params = json.loads(event['body']).get('test')
    if test_params:
        return {
            'statusCode': 200,
            'body': test_params
        }

    create_table()
    submit_report(event['body'])
    session.close()
    return {
        'statusCode': 200,
    }
