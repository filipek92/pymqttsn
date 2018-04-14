from .messages import *
from .tools import TopicTable
from threading import Event
from queue import Queue
from .timing import ThreadScheduler

import logging
from json import dumps, loads

import paho.mqtt.client as mqtt
import socket
import time

logger = logging.getLogger(__name__)

class Gateway():
    def __init__(self, phy, enable_advertising, gwId):
        self.phy = phy
        self.T_ADV = 15*60 if enable_advertising else None# 15 minutes
        self.N_ADV = 3 if enable_advertising else 0
        self.T_SEARCH_GWINFO = 5
        self.T_GWINFO = 5
        self.T_WAIT = 5*60
        self.T_RETRY = 10
        self.N_RETRY = 5
        self.gwId = gwId

        self.scheduler = ThreadScheduler()

        if enable_advertising:
            logger.info("Advertising enabled")
            self.scheduler.enter_repeated(self.T_ADV, 10, self.advertise)

    def advertise(self):
        m = MessageAdvertise(self.gwId, self.T_ADV)
        for i in range(self.N_ADV-1):
            self.scheduler.enter(i+1, 11, self.broadcast_packet, (m,))
        self.broadcast_packet(bytes(m))

class TransparentGatewayClient():
    def __init__(self, clientId, addr, mqttHost, mqttPort, gateway):
        self.mqttClient = mqtt.Client(client_id=clientId)
        self.clientId = clientId
        self.addr = addr
        self.topicTable = TopicTable()
        self.mqttClient.on_message = self.mqttOnMessage
        # self.mqttClient.on_connect = lambda client, userdata, flags, rc: print("Connect for {}".format(clientId))
        # self.mqttClient.on_disconnect = lambda client, userdata, rc: print("Disconnect for {}".format(clientId))
        self.mqttClient.loop_start()
        self.mqttHost = mqttHost
        self.mqttPort = mqttPort
        self.gateway = gateway
        self.logger = logger.getChild(clientId, )
        self.messages = Queue(100) 
        self.keepalive = 0
        self.sleeping = False
        self.messageQueue = Queue() 


    def mqttOnMessage(self, client, userdata, msg):
        try:
            self.logger.debug("Data {} on '{}'".format(msg.payload, msg.topic))

            topicId = self.topicTable.getTopicId(msg.topic)
            if topicId is None:
                topicId = self.topicTable.add(msg.topic)
                m = MessageRegister(topicName=msg.topic, msgId=Message.newMessageId(), topicId=topicId)
                m.addr = self.addr
                if not self.sleeping:
                    self.gateway._write(m)
                else:
                    self.messageQueue.put(m)

            m = MessagePublish(
                msgId=Message.newMessageId(),
                topicId=topicId,
                data=msg.payload,
                retain=msg.retain,
                qos=msg.qos)
            m.addr = self.addr
            if not self.sleeping:
                self.gateway._write(m)
            else:
                self.messageQueue.put(m)
        except Exception as e:
            self.logger.exception(e)

    def __repr__(self):
        return "{}(clientId='{}', addr={}, keepalive={}, sleeping={})".format(
                self.__class__.__name__,
                self.clientId,
                self.addr,
                self.keepalive,
                self.sleeping
            )

    def handle_packet(self, m):
        callback = {
            MessageRegister: self.onRegister,
            MessagePublish: self.onPublish,
            MessageSubscribe: self.onSubscribe,
            MessageUnsubscribe: self.onUnsubscribe,
            MessagePingReq: self.onPingReq, 
        }.get(type(m))
        if callback is not None:
            return callback(m)
        return None

    # def subscribe(self, topic, qos=0):
    #     result, mid = mqtt.Client.subscribe(self, topic, qos)
    #     return result, mid


    def getTopicById(self, topicId, topicType=0b00):
        if topicType == 0b10: # ShortTopic name, NotImplemented
            raise NotImplementedError("Short topic names not not implemented in {}".format(self.__class__.__name__))
        elif topicType == 0b01: # Predefined topics
            raise NotImplementedError("Short topic names not not implemented in {}".format(self.__class__.__name__))
        elif (topicType == 0b00) and (type(topicId) is int):
            return self.topicTable.getTopic(topicId)
        elif (topicType == 0b00) and (type(topicId) is str):
            return topicId
        else:
            raise ValueError("Topic type must be 2-bit number")

    def onConnect(self, clean=True, will=False):
        self.mqttClient._clean_session = clean
        self.mqttClient.connect(self.mqttHost, self.mqttPort)
        self.refreshTopicMap()
        self.sendQueue()

    def onDisconnect(self):
        self.mqttClient.disconnect()

    def onRegister(self, m):
        if not m.topicName in self.topicTable.topics:
            self.topicTable.add(m.topicName)
        return MessageRegAck(
            msgId=m.msgId,
            returnCode=0, 
            topicId=self.topicTable.getTopicId(m.topicName))

    def onPublish(self, m):
        topic = self.getTopicById(topicId=m.topicId, topicType=m.topicType)

        if topic is None:
            return MessagePubAck(msgId=m.msgId, topicId=m.topicId, returnCode=2)

        if m.flags.qos > 1:
            return MessagePubAck(msgId=m.msgId, topicId=m.topicId, returnCode=3) # TODO Support QoS 2

        self.logger.debug("Publishing to topic '{}'".format(topic))

        info = self.mqttClient.publish(topic=topic, payload=m.data, qos=m.flags.qos, retain=m.flags.retain)
        if m.flags.qos != 0:
            info.wait_for_publish()
            return MessagePubAck(msgId=m.msgId, topicId=m.topicId, returnCode=info.rc)

    def onSubscribe(self, m):
        if m.flags.topicType != 0:
            topic = self.getTopicById(m.topic, topicType=m.flags.topicType)
        else:
            topic = m.topic

        #Unknown topic
        if topic not in self.topicTable.topics:       
            self.topicTable.add(topic)

        self.logger.info("Subscribing topic '{}'".format(topic))

        result, mid = self.mqttClient.subscribe(topic, m.flags.qos)
        if ("+" in topic) or ("#" in topic): # Topic has wildcart
            topicId = 0
        else:
            topicId = self.topicTable.getTopicId(topic)

        return MessageSubAck(msgId=m.msgId, topicId=topicId, returnCode=result, qos=m.flags.qos)

    def onUnsubscribe(self, m):
        if m.flags.topicType != 0:
            topic = self.getTopicById(m.topic, topicType=m.flags.topicType)
        else:
            topic = m.topic

        self.logger.info("Unsubscribing topic '{}'".format(topic))

        result, mid = self.mqttClient.unsubscribe(topic)
        return MessageUnsubAck(msgId=m.msgId)

    def onPingReq(self, m):
        self.last_ping = time.monotonic()
        self.logger.debug("Ping")
        if self.sleeping:
            self.sendQueue()
        replyType = m.getReplyType() 
        return replyType()

    def sendQueue(self):
        while not self.messageQueue.empty():
            self.gateway._write(self.messageQueue.get())

    def refreshTopicMap(self):
        for topicId, topic in self.topicTable.items():
            s = MessageRegister(topicName=topic, msgId=Message.newMessageId(), topicId=topicId)
            s.addr = self.addr
            self.gateway._write(s)

    @property
    def ping_age(self):
        return time.monotonic() - self.last_ping

    def __del__(self):
        self.mqttClient.disconnect()
        self.mqttClient.loop_stop()

class TransparentGateway(Gateway):
    def __init__(self, phy, host="localhost", port=1883, enable_advertising=True, gwId=0):
        Gateway.__init__(self, phy=phy, enable_advertising=enable_advertising, gwId=0)
        self.clients = ClientList()
        self.sleepingClients = ClientList()
        self._stopEvent = Event()

        self.mqttHost = host
        self.mqttPort = port

        self.mqttClient = mqtt.Client()
        self.mqttClient.on_message = self.mqttOnMessage
        self.mqttClient.on_connect = self.mqttOnConnect
        self.mqttClient.loop_start()
        self.mqttClient.connect(host, port)

    def mqttOnConnect(self, client, userdata, flags, rc):
        client.subscribe("gateway/command")

    def mqttOnMessage(self, client, userdata, msg):
        try:
            data = loads(msg.payload.decode())
            logger.info("Gateway received message {} on topic '{}'".format(
                    data,
                    msg.topic,
                ))
            getattr(self, data["method"])(*data.get("args", ()), **data.get("kwargs", {}))
        except Exception as e:
            logger.exception(e)

    def dump(self):
        print("Client list:")
        for addr in self.clients:
            print("  {}: {}".format(addr, self.clients[addr]))

    def _read(self):
        data, addr = self.phy.read_packet()
        msg = Message.fromBinary(data)
        msg.addr = addr
        logger.debug("Read: {}".format(msg))
        return msg

    def _write(self, msg):
        logger.debug("Write: {}".format(msg))
        packet = bytes(msg)
        return self.phy.write_packet(packet, msg.addr)

    def handle_msg(self, msg):
        if type(msg) is MessageConnect:
            return self.onConnect(msg)
        if type(msg) is MessageDisconnect:
            return self.onDisconnect(msg)

        client = self.clients.get(msg.addr)
        if client:
            return client.handle_packet(msg)

        if type(msg) is MessagePingReq:
            client = self.sleepingClients.getClientById(msg.clientId)
            if client:
                return client.onPingReq(msg)

        replyType = msg.getReplyType()
        reply = replyType()
        if hasattr(reply, "returnCode"):
            reply.returnCode = 0xFE
        if hasattr(msg, "msgId") and hasattr(reply, "msgId"):
            reply.msgId = msg.msgId
        return reply

    def loop(self):
        logger.info("Gateway started")
        while not self._stopEvent.is_set():
            try:
                msg = self._read()
                reply = self.handle_msg(msg)
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.exception(e)
                replyType = msg.getReplyType()
                reply = replyType()
                if hasattr(reply, "returnCode"):
                    reply.returnCode = 0xFF
                if hasattr(msg, "msgId") and hasattr(reply, "msgId"):
                    reply.msgId = msg.msgId

            try:        
                if reply is not None:
                    reply.addr = msg.addr
                    self._write(reply)
            except Exception as e:
                logger.exception(e)

    def __del__(self):
        self.mqttClient.disconnect()
        self.mqttClient.loop_stop()

    def stop(self):
        self._stopEvent.set()

    def onConnect(self, m):
        clean = m.flags.clean
        will = m.flags.will
        keepalive = m.duration

        if will:
            return MessageConnAck(3)

        if clean:
            cl = self.clients.popById(m.clientId)

        try:
            client = self.sleepingClients.popById(m.clientId)
            if client is not None:
                client.keepalive = keepalive
                client.sleeping = False
                client.addr = m.addr
                client.onConnect(clean=clean, will=will, mqttConnect=False)
                client.logger.info("Sleeping client connected {}".format(client))
                self.clients.addClient(m.addr, client)
                return MessageConnAck(0)

            client = self.clients.popById(m.clientId)
            if client is None:
                client = TransparentGatewayClient(
                    m.clientId, 
                    m.addr, 
                    self.mqttHost, 
                    self.mqttPort, 
                    gateway=self)
                client.logger.info("New client connected {}".format(client))
            else:
                client.logger.info("Client reconnected")
            client.keepalive = keepalive
            client.addr = m.addr
            client.sleeping = False
            client.onConnect(clean = clean, will=will)
            self.clients.addClient(m.addr, client)
            return MessageConnAck(0)

        except socket.gaierror as e:
            logger.exception(e)
            return MessageConnAck(0xfd)
        except Exception as e:
            logger.exception(e)
        return MessageConnAck(255)

    def onDisconnect(self, m):
        if m.addr in self.clients:
            cl = self.clients.deleteClient(m.addr)
            if m.duration is None:
                logger.info("Client disconnected '{}'".format(cl))
                cl.onDisconnect()
            else:
                cl.sleeping = True
                cl.keepalive = m.duration
                self.sleepingClients.addClient(m.addr, cl)
                logger.info("Client made sleeping")
        return m

class ClientList(dict):
    def addClient(self, addr, client):
        dict.__init__(self)
        self[addr] = client

    def getClientById(self, clientId):
        for addr in self:
            c = self[addr]
            if c.clientId == clientId:
                return c
        return None

    def popById(self, clientId):
        for addr in self:
            if self[addr].clientId == clientId:
                return self.pop(addr)
        return None

    def getClientByAddr(self, addr):
        return self[addr]

    def deleteClient(self, addr):
        return self.pop(addr)