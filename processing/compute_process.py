import sys
import json
import logging
import os
import pandas as pd
from csv import DictWriter
from datetime import datetime, timedelta
from confluent_kafka import Consumer, Message
from confluent_kafka.error import KafkaError, KafkaException


with open('settings.json', 'r') as f:
    configs = json.load(f)
logging.basicConfig(format='%(asctime)s - %(name)s - %(message)s', level=logging.INFO, datefmt='%Y-%b-%d %H:%M:%S')
os.chdir(configs['os']['pwd'])
running = True
format = '%Y-%m-%d %H:%M:%S'
topic = ['crypto_ingestion']
scope = int(sys.argv[1])
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': f'{topic[0]}_consumer_group_{scope}',
    # 'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe(topic)


def consume_loop(consumer: Consumer, topics: str, scope: int) -> None:
    """
    Basic loop to consume messages.

    Args:
        `consumer` (Consumer): Consumer object to listen to specific topic.
        `topics` (str): Topic to listen to.

    Raises:
        KafkaException: Exception raised on error.
    """
    try:
        consumer.subscribe(topics)
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write(
                        '%% %s [%d] reached end at offset %d\n' %
                        (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                process_message(msg, scope)
    finally:
        consumer.close()


def process_message(msg: Message, scope: int) -> None:
    msg = json.loads(msg.value().decode('utf-8').replace("'", '"'))
    crypto = msg['symbol']
    d = dict()
    if scope == 1:
        save_csv_file(msg, scope)
    elif int(msg['end_time'].split(':')[-2]) % scope == 0:
        df = pd.read_csv('./data/1_min_crypto_data.csv')
        df['start_time'] = pd.to_datetime(df['start_time'], format=format)
        df['end_time'] = pd.to_datetime(df['end_time'], format=format)
        time_back = (datetime.strptime(msg['end_time'], 
            format) - timedelta(minutes=scope)).strftime(format)
        df = df[(df.start_time >= time_back) & (
            df.symbol == crypto)].sort_values(by=['end_time'], ascending=False)
        if df.shape[0] > 0:
            d['start_time'] = time_back
            d['end_time'] = msg['end_time']
            d['symbol'] = crypto
            d['open'] = df.open.iloc[-1]
            d['high'] = df.high.max()
            d['low'] = df.high.min()
            d['close'] = df.close.iloc[0]
            d['volume'] = df.volume.sum()
            d['trade_count'] = df.trade_count.sum()
            save_csv_file(d, scope)


def save_csv_file(d: dict, scope: int) -> None:
    cols = ["start_time", "end_time", "symbol", "open", "high", "low", "close",
            "volume", "trade_count"]
    with open(
            f'./data/{scope}_min_crypto_data.csv',
            'a') as csv_object:
        writer = DictWriter(csv_object, fieldnames=cols)
        writer.writerow(d)
        csv_object.close()
        logging.info(
            f'Data for {d["symbol"]} and {scope}min scope saved for period {d["start_time"]} --> {d["end_time"]}')


consume_loop(consumer, topic, scope)
