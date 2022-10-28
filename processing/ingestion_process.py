from alpaca.data.live import CryptoDataStream
from datetime import datetime, timedelta
from confluent_kafka import Producer
import os
import socket
import json
import logging


with open('settings.json', 'r') as f:
    configs = json.load(f)
os.chdir(configs['os']['pwd'])
logging.basicConfig(format='%(asctime)s - %(name)s - %(message)s', level=logging.INFO, datefmt='%Y-%b-%d %H:%M:%S')
api_key = configs['alpaca_settings']['api_key']
api_secret = configs['alpaca_settings']['api_secret']
topic = 'crypto_ingestion'
cryptos = ['BTC/USD', 'ETH/USD', 'LTC/USD', 'SOL/USD', 'BCH/USD', 'PAXG/USD']
format = '%Y-%m-%d %H:%M:%S'

producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname()})

wss_client = CryptoDataStream(api_key, api_secret)


async def data_handler(data) -> None:
    """
    Function to process incoming messages from web socket.

    For each message creates and sends dictionary with crypto data
    to kafka topic `crypto_ingestion` to either one of the two 
    partitions based on the symbols as key.

    Args:
        `data` (Any): Incoming data from web socket.
    """
    start_time = data.timestamp
    end_time = start_time + timedelta(minutes=1)
    start_time = datetime.strftime(start_time, format=format)
    end_time = datetime.strftime(end_time, format=format)
    d = {
        "start_time": start_time,
        "end_time": end_time,
        "symbol": data.symbol,
        "open": data.open,
        "high": data.high,
        "low": data.low,
        "close": data.close,
        "volume": data.volume,
        "trade_count": data.trade_count}
    producer.produce(topic=topic, value=str(
        d).encode('utf-8'), key=data.symbol,)
    logging.info(f'Message sent to Kafka topic {topic} for crypto {data.symbol}')


def subscribe(cryptos: list) -> None:
    """
    Function to iteratively subscribe to cryptos in cryptos list.

    Args:
        `cryptos` (list): List of str with cryptos symbols.
    """    
    for crypto in cryptos:
        wss_client.subscribe_bars(data_handler, crypto)


logging.info(f'Starting connection {socket.gethostname()}')
subscribe(cryptos)
wss_client.run()
