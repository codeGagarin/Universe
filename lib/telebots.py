from telegram import Update

from .bots import levels_alarm, perf_alarm

_bot_list = (levels_alarm, perf_alarm)
_bot_updaters = {}


def init(path_root):
    for bot in _bot_list:
        key, updater = bot.get_key_updater()
        _bot_updaters[key['token']] = updater
        web_hook_url = f"{key['web']}/{path_root}/{key['token']}"
        updater.bot.delete_webhook()
        updater.bot.set_webhook(web_hook_url)


def update(token, params):
    updater = _bot_updaters[token]
    if updater:
        updater.dispatcher.process_update(Update.de_json(params, updater.bot))


# import requests
# import unittest
#
#
# from .bots import LevelAlarm
# _bots = (LevelAlarm, )
# _update_func = {}
#
#
# _CMD_SET_HOOK = 'set_hook'
# _CMD_RM_HOOK = 'remove_hook'
# _CMD_SEND_MSG = 'sand_msg'
#
# def _get_api_url(token, method, params: dict = None):
#     url = f'https://api.telegram.org/bot{token}/'
#     params_str = '' if params is None else '?'+'&'.join(f'{key}={params[key]}' for key in params.keys())
#     cmd_list = {
#         _CMD_SET_HOOK: url + 'setWebhook' + params_str,
#         _CMD_RM_HOOK: url + 'setWebhook' + '&url=',
#         _CMD_SEND_MSG: url + 'sendMessage' + params_str,
#     }
#     return cmd_list[method]
#
#
# def sand_message(key, msg):
#     result = True
#     response = requests.get(_get_api_url(key['token'], _CMD_SEND_MSG, msg))
#     if not response.ok:
#         result = False
#     return result
#
#
# def update_webhooks(bot_root):
#     result = True
#     for bot in _bots:
#         bot._msg_sender = sand_message
#         key = bot.get_key()
#         _update_func[key['token']] = bot.update
#         token = key['token']
#         web_hook_url = f"{key['web']}/{bot_root}/{token}"
#         requests.get(_get_api_url(key['token'], _CMD_RM_HOOK))
#         response = requests.get(_get_api_url(key['token'], _CMD_SET_HOOK, {'url': web_hook_url}))
#         if not response.ok:
#             result = False
#     return result
#
#
# def update(token, msg):
#     _update_func[token](msg)
#
#
# class TelebotsTest(unittest.TestCase):
#     def test_update_webhooks(self):
#         self.assertTrue(update_webhooks('test_section'))
#
#
# if __name__ == '__main__':
#     unittest.main()
