import logging
import os

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

ADMIX_BOT_TOKEN = os.environ['ADMIX_BOT_TOKEN']


class AdmixBot:
    """
    Bot for sending aDMIX-related messages to a Slack channel.

    The bot currently hosts one generic function, send_message().
    """

    def __init__(self, channel_name, token=None, channel_key=None):
        if token is None:
            token = ADMIX_BOT_TOKEN
        self.client = WebClient(token=token)
        self.log = logging.getLogger('Slackbot')

        if channel_key is None:
            try:
                response = self.client.conversations_list(types='private_channel')
                channels = response['channels']
                next_cursor = response['response_metadata']['next_cursor']
                while next_cursor:
                    response = self.client.conversations_list(cursor=next_cursor)
                    channels += response['channels']
                    next_cursor = response['response_metadata']['next_cursor']
                channel_key = next((channel['id'] for channel in channels if channel['name'] == channel_name), None)
            except SlackApiError as e:
                print("Error joining channel: {}".format(e))
        self.channel_key = channel_key

    def send_message(self, message, channel_key=None, **kwargs):
        if channel_key is None:
            channel_key = self.channel_key
        try:
            response = self.client.chat_postMessage(channel=channel_key,
                                                    text=message,
                                                    **kwargs)
            self.log.debug('Got {} while writing {} to {}'.format(response, message, channel_key))
            return response
        except SlackApiError as e:
            print('Error sending message: {}'.format(e))
