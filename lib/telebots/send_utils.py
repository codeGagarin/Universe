"""
    Usage for collect chat_id for bot, send group messages, and checkup valid chat_id
"""

import psycopg2
from psycopg2 import sql

def collect_chat_id(bot_key, chat_id):
    db_key = bot_key['chat_id_key']
    connection = psycopg2.connect(dbname=db_key["db_name"], user=db_key["user"],
                                  password=db_key["pwd"], host=db_key["host"], port=db_key.get('port'))
    cursor = connection.cursor()

    pass

def send_message(bot_key, msg):
    pass