from telegram import Update

from . import send_utils

from . import levels_alarm, perf_alarm, starter_alarm

_bot_list = (levels_alarm, perf_alarm)
__all__ = _bot_list
_bot_updaters = {}


def init(path_root):
    # update web hooks
    for bot in _bot_list:
        key, updater = bot.get_key_updater()
        _bot_updaters[key['token']] = {'updater': updater, 'key': key}
        web_hook_url = f"{key['web']}/{path_root}/{key['token']}"
        updater.bot.delete_webhook()
        updater.bot.set_webhook(web_hook_url)


def update(token, params):
    bot_data = _bot_updaters.get(token)
    if bot_data:
        updater = bot_data['updater']
        bot_key = bot_data['key']
        send_utils.collect_chat_id()
        updater.dispatcher.process_update(Update.de_json(params, updater.bot))


