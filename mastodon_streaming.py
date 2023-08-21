import json
import os
from dotenv import load_dotenv

from kafka_writer import send_message_to_kafka

# Load environment variables from .env file
load_dotenv()

import logging
from mastodon import Mastodon, StreamListener

# Read environment variables
instance_url = os.environ.get('MASTODON_INSTANCE_URL', 'https://mastodon.social')
access_token = os.environ.get('MASTODON_ACCESS_TOKEN', 'token1')
tag = os.environ.get('HASHTAG')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MyStreamListener(StreamListener):
    def on_update(self, status):
        """
        Handle updates from the Mastodon stream.

        Args:
            status: Update status from the Mastodon stream.
        """
        data = {
            'text': status['content'],
            'id': status['id'],
            'created_at': status['created_at'].strftime("%Y-%m-%d, %H:%M:%S"),
            'timestamp': status['created_at'].strftime("%Y-%m-%d, %H:%M:%S"),
            'language': status['language'],
            'sensitive': status['sensitive'],
            'user_id': status['account']['id'],
            'url': status.tags[0].url,
            'application': status.application['name'],
        }
        json_data = json.dumps(data)
        send_message_to_kafka(data)

def main_streaming():
    """
    Main function to set up the Mastodon streaming.
    """
    # Create a Mastodon instance
    mastodon = Mastodon(
        access_token=access_token,
        api_base_url=instance_url
    )

    # Create a custom listener
    listener = MyStreamListener()

    # Start streaming hashtags
    try:
        stream_result = mastodon.stream_hashtag(
            tag,
            listener,
            local=False,
            run_async=False,
            timeout=300,
            reconnect_async=False,
            reconnect_async_wait_sec=5
        )
        logging.info("Streaming started for instance %s: %s", instance_url, stream_result)
    except Exception as e:
        logging.error("Error while starting streaming for instance %s: %s", instance_url, e)

if __name__ == '__main__':
    main_streaming()
