import logging
from threading import Thread, Event

from .messages import *
from .exceptions import MqttsnError
from .tools import TopicTable, WaitingList
from .timing import ThreadScheduler

logger = logging.getLogger(__name__)

class Client():
    T_RETRY = 10
    N_RETRY = 5

    def __init__(self, clientId, startThread=True):
        self.clientId = clientId
        self.state = "Disconnected"
        self.topicTable = TopicTable()
        self.onMessage = None
        self.waitingList = WaitingList()
        self.thread = None
        self.stop_event = Event()
        self.keepalive = 0
        if startThread:
            self.start()

        self.scheduler = ThreadScheduler()
        self.events = {'ping': None}

    def start(self):
        if self.thread is None:
            self.thread = Thread(target=self.loop, daemon=True)
            self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.thread.join()

    def loop(self):
        while not self.stop_event.is_set():
            try:
                data, addr = self.read_packet()
                msg = Message.fromBinary(data)
                logger.debug("Read: {}".format(msg))
                msgId = msg.msgId if hasattr(msg, "msgId") else None

                ev = self.waitingList.get_waiting(addr, type(msg), msgId)
                if ev:
                    ev.setReply(msg)
                    continue
                self.handle_message(msg)
            except Exception as e:
                logging.exception(e)

    def _write_read(self, msg, retry=None):
        we = self.waitingList.add_waiting(
            address=self.server_addr,
            waitfor=msg.getReplyType(), 
            msgId=msg.msgId if hasattr(msg, "msgId") else None
        )

        retry = self.N_RETRY

        while True:
            self._write(msg)
            if we.wait(timeout=self.T_RETRY):
                return we.reply
            if retry <= 0:
                raise TimeoutError("Server not respond")
            retry -= 1
            if hasattr(msg, "flags"):
                msg.flags.dup = True

    def _write(self, msg):
        logger.debug("Write: {}".format(msg))
        return self.write_packet(bytes(msg))

    def handle_message(self, msg):
        callback = {
            MessagePublish: self.onPublish,
            MessageRegister: self.onRegister
        }.get(type(msg))
        if callback:
            callback(msg)

    def connect(self, will=False, clean=False, keepalive=600):
        self.keepalive = keepalive
        s = MessageConnect(self.clientId, will=will, clean=clean, duration=keepalive)
        r = self._write_read(s)
        MqttsnError.raiseIfReturnCode(r.returnCode)
        logger.info("Connected to server")
        self.state = "Connected"
        self.events["ping"] = self.scheduler.enter_repeated(keepalive*0.75, 10, self.ping)

    def register(self, topic):
        s = MessageRegister(topicName=topic)
        r = self._write_read(s)
        MqttsnError.raiseIfReturnCode(r.returnCode)
        self.topicTable.add(topic, r.topicId)
        return r.topicId

    def publish(self, topic, data, retain=False, qos=0, topicType=None):
        if topicType is None or topicType == Message.NORMAL_TOPIC: # Normal topic (Registered topic ID)
            if type(topic) == int:
                topicId = topic
            elif type(topic) == str:
                topicId = self.topicTable.getTopicId(topic)
                if topicId is None:
                    topicId = self.register(topic)
            else:
                raise TypeError("Topic for topicType=0b00 must be str or int")
        elif topicType == Message.PREDEFINED_TOPIC: # Predefined topic
            raise NotImplementedError("Predefined topics are not implemented")
        elif topicType == Message.SHORT_TOPIC: # Short topic
            raise NotImplementedError("Short topics are not implemented")
        else:
            raise RuntimeError("Topic type must be 0b00, 0b01, 0b10 or None")

        s = MessagePublish(topicId=topicId, data=data, retain=retain, qos=qos)
        if qos == 0:
            self._write(s)
        else:
            r = self._write_read(s)
            MqttsnError.raiseIfReturnCode(r.returnCode)

    def subscribe(self, topic, qos=0):
        s = MessageSubscribe(topic=topic, qos=qos, topicType=0)
        r = self._write_read(s)
        if r.topicId !=0:
            self.topicTable.add(topic, r.topicId)
            return r.topicId

    def unsubscribe(self, topic):
        s = MessageUnsubscribe(topic=topic, topicType=0)
        r = self._write_read(s)

    def disconnect(self, duration=None):
        if self.state != "Disconnected":
            s = MessageDisconnect(duration)
            self._write_read(s)
            self.state = "Disconnected"
            self.events["ping"].cancel()
            self.events["ping"] = None

    def ping(self):
        s = MessagePingReq(clientId=self.clientId if self.state != "Connected" else None)
        try:
            r = self._write_read(s)
        except TimeoutError:
            logger.info("Connection to server lost")
            self.state = "Disconnected"
            self.events["ping"].cancel()
            self.events["ping"] = None

    def onPublish(self, msg):
        msg.topic = self.topicTable.getTopic(msg.topicId)
        # TODO Ask if unknown

        # if topic is None:
        #     return MessagePubAck(msgId=m.msgId, topicId=m.topicId, returnCode=2)

        if self.onMessage:
            self.onMessage(msg)
        else:
            logger.info(msg)

    def onRegister(self, msg):
        self.topicTable.add(topic=msg.topicName, id=msg.topicId)
        logger.info("New topic registered: {} ==> {}".format(msg.topicName, msg.topicId))
        s = MessageRegAck(
            msgId=msg.msgId,
            returnCode=0,
            topicId=msg.topicId)
        self._write(s)

    def __del__(self):
        #self.disconnect()
        pass
