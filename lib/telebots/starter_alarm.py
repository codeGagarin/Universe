from telegram.ext import CommandHandler, Updater, CallbackQueryHandler

from keys import KeyChain

_key = KeyChain.TGB_STARTER_ALARM
_updater = Updater(token=_key['token'], use_context=True)
_dispatcher = _updater.dispatcher
_sender = None

def get_key_updater(sender):
    """ External callback """

    return _key, _updater

def send_message(msg):
    pass
