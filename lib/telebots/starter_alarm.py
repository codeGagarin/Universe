from telegram.ext import Updater

from keys import KeyChain
from lib.abs import Alarmer

_key = KeyChain.TGB_STARTER_ALARM
_updater = Updater(token=_key['token'], use_context=True)
_dispatcher = _updater.dispatcher
_sender = None


class TelegramAlarmer(Alarmer):
    def alarm(self, msg):
        _updater.bot.send_message(194429825, msg)
