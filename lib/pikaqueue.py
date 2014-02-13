import pika
import time
import pickle
import libnntscclient.logger as logger
from libnntsc.configurator import get_nntsc_config

class PikaBasic(object):
    def __init__(self, exchange, host, port, ssl, user, pword):
        self._connection = None
        self._channel = None
        self._host = host
        self._port = port
        self._ssl = ssl
        self._credentials = pika.PlainCredentials(user, pword)
        self._exchangename = exchange
    
    def _pikaConnect(self, host, port, ssl, creds):
        attempts = 1
        connection = None
        while True:
            try:
                connection = pika.BlockingConnection(
                        pika.ConnectionParameters(host=host,
                                port=int(port),
                                ssl=ssl,
                                credentials=creds,
                                retry_delay=5))
                break
            except Exception:
                delay = attempts * 10.0
                if delay > 120.0:
                    delay = 120.0
                    logger.log("PikaPublisher failed to connect to RabbitMQ (attempt %d), trying again in %.0f seconds" % (attempts, delay))
                    time.sleep(delay)
                    attempts += 1
                    continue

        return connection
    
    def connect(self):
        self._connection = self._pikaConnect(self._host, self._port, self._ssl,
                self._credentials)
        self._channel = self._connection.channel()

        if self._exchangename != '':
            self._channel.exchange_declare(exchange=self._exchangename,
                    type='direct')
        
   

class PikaPublisher(PikaBasic):
    
    def __init__(self, exchange, key, host, port, ssl, user, pword):
        super(PikaPublisher, self).__init__(exchange, host, port,
                ssl, user, pword)
        self._pubkey = key

    def publish_data(self, data, contenttype, key=None):

        if key == None:
            key = self._pubkey

        return self._channel.basic_publish(exchange=self._exchangename, 
                routing_key=key,
                body=data,
                properties=pika.BasicProperties(delivery_mode=2,
                        content_type=contenttype))

    def publishStream(self, colid, colname, streamid, streamprops):
        content = (1, (colid, colname, streamid, streamprops))
        pubstring = pickle.dumps(content)
        self.publish_data(pubstring, "text/plain") 

    def publishLiveData(self, colname, stream, ts, result):
        content = (0, (colname, stream, ts, result))
        pubstring = pickle.dumps(content)
        self.publish_data(pubstring, "text/plain") 

    def publishPush(self, colid, ts):
        content = (2, (colid, ts))
        pubstring = pickle.dumps(content)
        self.publish_data(pubstring, "text/plain") 

    def halt_publisher(self):
        self._connection.close()

class PikaConsumer(PikaBasic):
    def __init__(self, exchange, queuename, host, port, ssl, user, pword):
        super(PikaConsumer, self).__init__(exchange, host, port,
                ssl, user, pword)
        self._queue = None
        self._queuename = queuename

    def configure_consumer(self, callback):
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(callback, queue=self._queuename)

    def run_consumer(self):
        while 1:
            try:
                self._channel.start_consuming()
            except pika.exceptions.ConnectionClosed:
                self._channel.close()
                self.connect()
                continue
            except:
                self._channel.stop_consuming()
                break

        self._connection.close()
    
    def bind_queue(self, key): 
        if self._queue == None:
            res = self._channel.queue_declare(queue=self._queuename, durable=True)
            self._queue = res.method.queue
        
        self._channel.queue_bind(exchange = self._exchangename, 
                queue=self._queue, routing_key=key)

    def unbind_queue(self, key):
        if self._queue == None:
            return

        self._channel.queue_unbind(queue=self._queue, 
                exchange=self._exchangename, routing_key=key)


def parseExportOptions(conf):
    username = get_nntsc_config(conf, "liveexport", "username")
    if username == "NNTSCConfigError":
        logger.log("Invalid username option for live exporter")
        return None, None, None
    password = get_nntsc_config(conf, "liveexport", "password")
    if password == "NNTSCConfigError":
        logger.log("Invalid password option for live exporter")
        return None, None, None
    port = get_nntsc_config(conf, "liveexport", "port")
    if port == "NNTSCConfigError":
        logger.log("Invalid port option for live exporter")
        return None, None, None

    return username, password, port 

def initExportPublisher(conf, key, exchange):
    username, password, port = parseExportOptions(conf)
    if username == None:
        return None

    exporter = PikaPublisher(exchange, key, 'localhost', port, False, 
            username, password)
    if exporter == None:
        logger.log("Failed to create live exporter for %s -- no live export will occur" % (queuename))
    else:
        exporter.connect()
    return exporter


def initExportConsumer(conf, queuename, exchange):
    username, password, port = parseExportOptions(conf)
    if username == None:
        return None

    consumer = PikaConsumer(exchange, queuename, 'localhost', port, False, 
            username, password)
    if consumer == None:
        logger.log("Failed to create live consumer for %s -- no live export will occur" % (queuename))
    else:
        consumer.connect()
    return consumer

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
