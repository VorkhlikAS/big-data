from kafka import KafkaConsumer
from functools import wraps
import time
from random import randint


def backoff(tries: int, sleep: int) -> callable:
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt_num in range(tries):
                try:
                    return func(*args, **kwargs)
                except Exception as error:
                    if attempt_num < tries - 1:
                        time.sleep(sleep)
                    else:
                        raise error
        return wrapper
    return decorator

                    
@backoff(tries=10,sleep=60)
def message_handler(value)->None:
    error_prob = 7
    if randint(0, 10) < error_prob:
        raise Exception
    else:
        pass
    print(value)


def create_consumer():
    print("Connecting to Kafka brokers")
    consumer = KafkaConsumer("test_topic",
                             group_id='test_topic_group',
                             bootstrap_servers='localhost:29092',
                             auto_offset_reset='earliest',
                             enable_auto_commit=True)

    for message in consumer:
        # send to http get (rest api) to get response
        # save to db message (kafka) + external
        message_handler(message)
        print(message)




if __name__ == '__main__':
    create_consumer()
