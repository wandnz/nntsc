import pika
import time
import pickle
import libnntscclient.logger as logger
from libnntsc.configurator import get_nntsc_config
import logging

from multiprocessing import Queue, Process
from threading import Thread
import Queue as StdQueue

PIKA_CONSUMER_HALT = 0
PIKA_CONSUMER_RETRY = 1
PIKA_CONSUMER_OK = 2

class PikaNNTSCException(Exception):
    def __init__(self, retry):
        self.retry = retry
    def __str__(self):
        if self.retry:
            return "NNTSC Exception encountered while reading Rabbit Queue -- retrying"
        else:
            return "NNTSC Exception encountered while reading Rabbit Queue -- halting"

class PikaBasicAsync(object):
    def __init__(self, exchange, queuename, host, port, ssl, user, pword):
        self._connection = None
        self._channel = None
        self._host = host
        self._port = port
        self._ssl = ssl
        self._credentials = pika.PlainCredentials(user, pword)
        self._exchangename = exchange
        self._closing = False
        self._queuename = queuename
    
        logging.basicConfig()

    def _pikaConnect(self, host, port, ssl, creds):
        connection = pika.SelectConnection(
                pika.ConnectionParameters(host=host,
                        port=int(port),
                        ssl=ssl,
                        credentials=creds,
                        retry_delay=5), self._pikaConnectionOpen,
                        stop_ioloop_on_close=False)
        return connection
    
    def connect(self):
        self._connection = self._pikaConnect(self._host, self._port, self._ssl,
                self._credentials)

    def reconnect(self):
        self._connection.ioloop.stop()
        if not self._closing:
            self._connection = self._pikaConnect(self._host, self._port, 
                    self._ssl, self._credentials)
            self._connection.ioloop.start()
    
    def close_channel(self):
        if self._channel:
            self._channel.close()

    def close_connection(self):
        self._closing = True
        self._connection.close()
    
    def _pikaCancelled(self, unused):
        self.close_channel()    

    def _pikaConnectionOpen(self, unused):
        self._connection.add_on_close_callback(self._pikaConnectionClosed)
        self._connection.channel(on_open_callback=self._pikaChannelOpen)

    def _pikaConnectionClosed(self):
        logger.log("Pika connection was closed")

        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            logger.log("Pika connection closed, retrying")
            self._connection.add_timeout(5, self.reconnect)

    def _pikaChannelOpen(self, channel):

        self._channel = channel
        self._channel.add_on_close_callback(self._pikaChannelClosed)

        if self._exchangename != '':
            self._channel.exchange_declare(self._pikaExchangeDeclared, 
                    exchange=self._exchangename,
                    type='direct')
        else:
            self._channel.queue_declare(self._pikaQueueDeclared, 
                    self._queuename, durable = True)
        
    def _pikaChannelClosed(self, channel, replycode, replytext):
        logger.log("Pika Channel was closed: %s %s" % (replycode, replytext))
        if not self._closing:
            self._connection.close()

    def _pikaExchangeDeclared(self, unused):
        self._channel.queue_declare(self._pikaQueueDeclared, self._queuename,
                durable = True)

    def _pikaQueueDeclared(self, methodframe):
        return 

    def run(self):
        self.connect()
        self._connection.ioloop.start()

class PikaPublisher(PikaBasicAsync):
    
    def __init__(self, exchange, queuename, key, host, port, ssl, user, pword,
            sourcequeue):
        super(PikaPublisher, self).__init__(exchange, queuename, host, port,
                ssl, user, pword)
        self._pubkey = key
        self._halted = False
        self._stopping = False

        self._outstanding = sourcequeue

    def halt_publisher(self):
        self._stopping = True
        self.close_channel()
        self.close_connection()
        
        # This will cause the IO loop to restart and close our connection
        # to rabbitMQ nicely
        self._connection.ioloop.start()


    def _pikaQueueDeclared(self, unused):
        self._channel.queue_bind(self._pikaQueueBound, self._queuename,
                self._exchangename, self._pubkey)

    def _pikaQueueBound(self, unused):
        logger.log("Ready to start publishing")
        self._publish()

    def _publish(self):
        while 1:
            if self._stopping:
                return
            
            try:
                pubstring = self._outstanding.get(False)
            except StdQueue.Empty:
                continue
            
            self._channel.basic_publish(self._exchangename, self._pubkey,
                    pubstring, 
                    pika.BasicProperties(delivery_mode=2,
                        content_type = "text/plain"))
    

class PikaPubQueue(object):
    def __init__(self, sourcequeue):
        self._outstanding = sourcequeue

    def publishStream(self, colid, colname, streamid, streamprops):
        content = (1, (colid, colname, streamid, streamprops))
        pubstring = pickle.dumps(content)
        try:
            self._outstanding.put(pubstring, True, 10)
        except StdQueue.Full:
            logger.log("Internal publishing queue has reached capacity!")
            return -1
        return 0

    def publishLiveData(self, colname, stream, ts, result):
        content = (0, (colname, stream, ts, result))
        pubstring = pickle.dumps(content)
        try:
            self._outstanding.put(pubstring, True, 10)
        except StdQueue.Full:
            logger.log("Internal publishing queue has reached capacity!")
            return -1
        return 0

    def publishPush(self, colid, ts):
        content = (2, (colid, ts))
        pubstring = pickle.dumps(content)
        try:
            self._outstanding.put(pubstring, True, 10)
        except StdQueue.Full:
            logger.log("Internal publishing queue has reached capacity!")
            return -1
        return 0


class PikaConsumer(PikaBasicAsync):
    def __init__(self, exchange, queuename, host, port, ssl, user, pword):
        super(PikaConsumer, self).__init__(exchange, queuename, host, port,
                ssl, user, pword)
        self._consumer_tag = None
        self._unbound = []
        self._keys = []
        self._prefetch = 1
        self.callback = None
    
    def halt_consumer(self):
        self._closing = True
        if self._channel:
            self._channel.basic_cancel(self._pikaCancelled, self._consumer_tag)
        
        # This will cause the IO loop to restart and close our connection
        # to rabbitMQ nicely
        self._connection.ioloop.start()


    def _pikaQueueDeclared(self, unused):

        if len(self._unbound) == 0:
            #logger.log("Warning: no valid routing keys to consume from")
            self._start_consume()
            return

        self._bind_next_key()

    def _pikaQueueBound(self, unused):
        if len(self._unbound) == 0:
            self._start_consume()
            return

        self._bind_next_key()

    def _bind_next_key(self):
        if len(self._unbound) == 0:
            return 0

        nextkey = self._unbound[0]
        self._channel.queue_bind(self._pikaQueueBound, self._queuename,
                self._exchangename, nextkey)
        self._unbound = self._unbound[1:]
        return 1

    def _start_consume(self):
        self._channel.basic_qos(prefetch_count=self._prefetch)
        self._channel.add_on_cancel_callback(self._pikaCancelled)
        logger.log("Started consuming from %s" % (self._queuename))
        self._consumer_tag = self._channel.basic_consume(self.callback, 
                self._queuename)

    
    def configure(self, keys, callback, prefetch):
        self._keys = keys[:]
        self._unbound = keys[:]
        self.callback = callback
        self._prefetch = prefetch
        
    

def parseExportOptions(conf):
    username = get_nntsc_config(conf, "liveexport", "username")
    if username == "NNTSCConfigError" or username == "NNTSCConfigMissing":
        logger.log("Invalid username option for live exporter")
        return None, None, None
    password = get_nntsc_config(conf, "liveexport", "password")
    if password == "NNTSCConfigError" or username == "NNTSCConfigMissing":
        logger.log("Invalid password option for live exporter")
        return None, None, None
    port = get_nntsc_config(conf, "liveexport", "port")
    if port == "NNTSCConfigMissing":
        port = 5672
    if port == "NNTSCConfigError":
        logger.log("Invalid port option for live exporter")
        return None, None, None


    return username, password, port 

def startPubThread(conf, key, exchange, queuename, src):
    username, password, port = parseExportOptions(conf)
    if username == None:
        return None

    
    exporter = PikaPublisher(exchange, queuename, key, 'localhost', port, 
            False, username, password, src)
    if exporter == None:
        logger.log("Failed to create live exporter for %s -- no live export will occur" % (queuename))

    try:
        exporter.run()
    except KeyboardInterrupt:
        exporter.halt_publisher()
    except:
        logger.log("Unknown exception in publisher thread (key=%s)" % (key))
        raise


def initExportPublisher(conf, key, exchange, queuename):
    src = Queue(200000)
    p = Thread(target=startPubThread, \
            args=(conf, key, exchange, queuename, src))
    p.start()
    publisher = PikaPubQueue(src)

    return publisher, p


def initExportConsumer(conf, queuename, exchange):
    username, password, port = parseExportOptions(conf)
    if username == None:
        return None

    consumer = PikaConsumer(exchange, queuename, 'localhost', port, False, 
            username, password)
    if consumer == None:
        logger.log("Failed to create live consumer for %s -- no live export will occur" % (queuename))
    return consumer

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
