from telegram.ext import CommandHandler, Updater, CallbackQueryHandler

from keys import KeyChain

_key = KeyChain.TGB_PERF_ALARM
_updater = Updater(token=_key['token'], use_context=True)
_dispatcher = _updater.dispatcher


def get_key_updater(sender):
    """ External callback """
    return _key, _updater


def _start(update, context):
    print(update)


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
