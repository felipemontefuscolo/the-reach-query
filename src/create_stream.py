import sys
import time
import json
import os
import numpy
from math import *
from collections import OrderedDict
import sys
import six
from datetime import datetime
from kafka.client import KafkaClient
from kafka.producer import KeyedProducer
import glob


class Producer(object):

    def __init__(self, addr):
        self.client = KafkaClient(addr)
        self.producer = KeyedProducer(self.client)

    def produce_msgs(self, source_symbol):

        data_path = "/home/ubuntu/db/test01"

        for fn in sorted(glob.glob(data_path + '/*.dat')):
            with open(fn, "r") as ff:
                for l in ff:
                    self.producer.send_messages('graph1', source_symbol, l)
        

        #while True:
        #    time_field = datetime.now().strftime("%Y%m%d %H%M%S")
        #    price_field += random.randint(-10, 10)/10.0
        #    volume_field = random.randint(1, 1000)
        #    str_fmt = "{};{};{};{}"
        #    message_info = str_fmt.format(source_symbol,
        #                                  time_field,
        #                                  price_field,
        #                                  volume_field)
        #    print message_info
        #    self.producer.send_messages('graph1', source_symbol, message_info)
        #    msg_cnt += 1

if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key)




