#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import commands
import logging

import os
import re
import socket
import string
import time
import copy
import traceback

logging.basicConfig(
    filename = '/tmp/mongodb_monitor-%s.log' % time.strftime('%Y-%m-%d'),
    filemode = 'a',
    level = logging.NOTSET,
    format = '%(asctime)s - %(levelname)s: %(message)s'
)


IGNORE_KEYS = [
    # instance information
    "host",
    "version",
    "process",
    "pid",
    "uptime",
    "uptimeMillis",
    "uptimeEstimate",
    "localTime",
]

METRICS = {
    'opcounters_insert': 'COUNTER',
    'opcounters_query': 'COUNTER',
    'opcounters_update': 'COUNTER',
    'opcounters_delete': 'COUNTER',
    'opcounters_getmore': 'COUNTER',
    'opcounters_command': 'COUNTER',

    'mem_mapped': 'GAUGE',
    'mem_virtual': 'GAUGE',
    'mem_resident': 'GAUGE',
    'extra_info_page_faults': 'GAUGE',

    "globalLock_currentQueue_total": "GAUGE",
    "globalLock_currentQueue_readers": "GAUGE",
    "globalLock_currentQueue_writers": "GAUGE",
    "globalLock_activeClients_total": "GAUGE",
    "globalLock_activeClients_readers": "GAUGE",
    "globalLock_activeClients_writers": "GAUGE",
    "connections_current": "GAUGE",
}

class MongoMonitor(object):
    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.hostname = socket.gethostname()
        self.username = username
        self.password = password
        self.tags = 'port=%s' % port

    def flatten(self, d, pre = '', sep = '_'):
        """Flatten a dict (i.e. dict['a']['b']['c'] => dict['a_b_c'])"""

        new_d = {}
        for k,v in d.items():
            if type(v) == dict:
                new_d.update(self.flatten(d[k], '%s%s%s' % (pre, k, sep)))
            else:
                new_d['%s%s' % (pre, k)] = v
        return new_d

    def _collect(self):
        mongo_info = {}
        if self.username and self.password:
            cmd = '/usr/local/mongodb/bin/mongo --host %s --port %s -u %s -p %s --quiet --eval "printjson(db.serverStatus())"' % (self.host, self.port, self.username, self.password)
        else:
            cmd = '/usr/local/mongodb/bin/mongo --host %s --port %s --quiet --eval "printjson(db.serverStatus())"' % (self.host, self.port)

        status, output = commands.getstatusoutput(cmd)
        if status != 0:
            mongo_info['alive'] = 0
            return mongo_info
        else:
            mongo_info['alive'] = 1

        metrics_str = re.sub('\w+\((.*)\)', r"\1", output) # remove functions
        metrics = self.flatten(json.loads(metrics_str))

        for key in metrics:
            if key in IGNORE_KEYS:
                continue
            else:
                mongo_info[key] = metrics[key]
        return mongo_info

    def run(self):
        mongo_info = self._collect()
        payload = []
        ts = int(time.time())
        for key in mongo_info:
            metric = 'mongo.%s' % key.replace('_', '.')
            if key in METRICS:
                item = {
                    'endpoint': self.hostname,
                    'metric': metric,
                    'tags': self.tags,
                    'timestamp': ts,
                    'value': mongo_info[key],
                    'step': 60,
                    'counterType': METRICS[key]
                }
                payload.append(item)
        return payload

def push(payload):
    if payload:
        try:
            r = requests.post("http://127.0.0.1:1988/v1/push", data=json.dumps(payload))
            logging.info('push data status: %s' % r.text)
        except:
            logging.warn('push data status failed, Exception: %s' % traceback.format_exc())

def main():
    mongo_instances = [
        {'host': '127.0.0.1', 'port': 27017, 'username': None, 'password': None}
    ]
    payload = []

    for instance in mongo_instances:
        try:
            host = instance['host']
            port = instance['port']
            username = instance['username']
            password = instance['password']
            mongo_monitor = MongoMonitor(host, port, username, password)
            payload.extend(mongo_monitor.run())
        except:
            logging.info('Exception: %s' % traceback.format_exc())
    logging.info('payload length: %s' % len(payload))
    push(payload)


if __name__ == '__main__':
    start = time.time()
    main()
    logging.info('Cost: %s' % (time.time() - start))
