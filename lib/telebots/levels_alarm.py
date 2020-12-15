from telegram.ext import CommandHandler, Updater, CallbackQueryHandler
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from keys import KeyChain

_key = KeyChain.TGB_LEVEL_ALARM
_updater = Updater(token=_key['token'], use_context=True)
_dispatcher = _updater.dispatcher


def get_key_updater():
    """ External callback """
    return _key, _updater


def _start(update, context):
    # context.bot.send_message(chat_id=update.effective_chat.id, text="Alarm subscription -- Ready")
    keyboard = [
        [
            InlineKeyboardButton("Option 1", callback_data='1'),
            InlineKeyboardButton("Option 2", callback_data='2'),
        ],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    update.message.reply_text('Alarm subsciption -- Ok', reply_markup=reply_markup)


def _button(update, context):
    query = update.callback_query
    query.answer()
    query.edit_message_text(text="Selected option: {}".format(query.data))


def _add_handler(disp):
    disp.add_handler(CommandHandler('start', _start))
    disp.add_handler(CallbackQueryHandler(_button))

def alarm(msg):
    # 194429825
    _updater.bot.send_message(194429825, msg)

_add_handler(_dispatcher)


