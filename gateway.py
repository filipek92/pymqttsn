from .messages import *
from .tools import TopicTable
from threading import Event
from queue import Queue
from .timing import ThreadScheduler

import logging
from json import dumps, loads

import paho.mqtt.client as mqtt
import socket

logger = logging.getLogger(__name__)

class Gateway():
    # scheduler = None

    def __init__(self, enable_advertising, gwId):
        self.T_ADV = 15*60 if enable_advertising else None# 15 minutes
        self.N_ADV = 3 if enable_advertising else 0
        self.T_SEARCH_GWINFO = 5
        self.T_GWINFO = 5
        self.T_WAIT = 5*60
        self.T_RETRY = 10
        self.N_RETRY = 5
        self.gwId = gwId
        # if Gateway.scheduler is None:
        #     Gateway.scheduler = ThreadScheduler()
    #     if self.T_ADV is not None and self.N_ADV > 0:
    #         logger.info("Advertising enabled")
    #         self.advertising(True)

    # def advertising(self, reschedule=False):
    #     if reschedule:
    #         for i in range(self.N_ADV):
    #             self.scheduler.enter(self.T_ADV+i, 10, self.advertising, (i==0,))

    #     m = MessageAdvertise(self.gwId, self.T_ADV)

    #     self.broadcast_packet(bytes(m))

    def read_packet(self):
        raise RuntimeError("Subclasses of pymqttsn.Gateway must implement method read_packet(), which class {} doesnt".format(self.__class__.__name__))

    def write_packet(self, data, addr=None):
        raise RuntimeError("Subclasses of pymqttsn.Gateway must implement method write_packet(data, addr), which class {} doesnt".format(self.__class__.__name__))

    def broadcast_packet(self, data, radius=None):
        raise RuntimeError("Subclasses of pymqttsn.Gateway must implement method write_packet_broadcast(data, addr), which class {} doesnt".format(self.__class__.__name__))

class TransparentGatewayClient(mqtt.Client):
    def __init__(self, clientId, addr, mqttHost, mqttPort, gateway):
        mqtt.Client.__init__(self)
        self.clientId = clientId
        self.addr = addr
        self.topicTable = TopicTable()
        self.on_message = self.mqttOnMessage
        self.loop_start()
        self.connect(mqttHost, mqttPort)
        self.gateway = gateway
        self.logger = logger.getChild(clientId)
        self.messages = Queue() 


    def mqttOnMessage(self, client, userdata, msg):
        try:
            self.logger.info("Received message {} on topic '{}' with QoS {}".format(
                msg.payload,
                msg.topic,
                msg.qos
            ))

            topicId = self.topicTable.getTopicId(msg.topic)
            if topicId is None:
                topicId = self.topicTable.add(msg.topic)
                m = MessageRegister(topicName=msg.topic, msgId=Message.newMessageId(), topicId=topicId)
                m.addr = self.addr
                self.gateway._write(m)

            m = MessagePublish(
                msgId=Message.newMessageId(),
                topicId=topicId,
                data=msg.payload,
                retain=msg.retain,
                qos=msg.qos)
            m.addr = self.addr
            self.gateway._write(m)
        except Exception as e:
            self.logger.exception(e)

    def __repr__(self):
        return "{}(clientId='{}', addr={})".format(
                self.__class__.__name__,
                self.clientId,
                self.addr
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

    def subscribe(self, topic, qos=0):
        result, mid = mqtt.Client.subscribe(self, topic, qos)
        self.logger.info("Subscribing topic '{}' with result {}".format(topic, result))
        return result, mid


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

        info = self.publish(topic=topic, payload=m.data, qos=m.flags.qos, retain=m.flags.retain)
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

        result, mid = self.subscribe(topic, m.flags.qos)
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

        result, mid = self.unsubscribe(topic)
        return MessageUnsubAck(msgId=m.msgId)

    def onPingReq(self, m):
        pass

    def __del__(self):
        self.disconnect()
        self.loop_stop()

class TransparentGateway(Gateway):
    def __init__(self, host="localhost", port=1883, enable_advertising=True, gwId=0):
        Gateway.__init__(self, enable_advertising=enable_advertising, gwId=0)
        self.clients = ClientList()
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
        data, addr = self.read_packet()
        msg = Message.fromBinary(data)
        msg.addr = addr
        logger.debug("Read: {}".format(msg))
        return msg

    def _write(self, msg):
        logger.debug("Write: {}".format(msg))
        packet = bytes(msg)
        return self.write_packet(packet, msg.addr)

    def loop(self):
        logger.info("Gateway started")
        while not self._stopEvent.is_set():
            try:
                msg = self._read()
                if type(msg) is MessageConnect:
                    reply = self.onConnect(msg)
                elif type(msg) is MessageDisconnect:
                    reply = self.onDisconnect(msg)
                else:
                    client = self.clients.get(msg.addr)
                    if client:
                        reply = client.handle_packet(msg)
                    else:
                        replyType = msg.getReplyType()
                        reply = replyType()
                        if hasattr(reply, "returnCode"):
                            reply.returnCode = 0xFE
                        if hasattr(msg, "msgId") and hasattr(reply, "msgId"):
                            reply.msgId = msg.msgId
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

        if will:
            return MessageConnAck(3)

        if clean:
            cl = self.clients.popById(m.clientId)

        try:
            client = self.clients.popById(m.clientId)
            if client is None:
                client = TransparentGatewayClient(
                    m.clientId, 
                    m.addr, 
                    self.mqttHost, 
                    self.mqttPort, 
                    gateway=self)
            else:
                client.addr = m.addr
                #Resend all topic maps
                for topicId, topic in client.topicTable.items():
                    s = MessageRegister(topicName=topic, msgId=Message.newMessageId(), topicId=topicId)
                    s.addr = client.addr
                    self._write(s)
                    #self._read(waitfor=MessageRegAck, msgId=s.msgId)

            self.clients.addClient(m.addr, client)
            return MessageConnAck(0)
        except socket.gaierror as e:
            logger.exception(e)
            return MessageConnAck(0xfd)
        except Exception as e:
            logger.exception(e)
        return MessageConnAck(255)

    def onDisconnect(self, m):
        if m.duration is not None:
            raise NotImplementedError("Support for sleeping clients not implemented")
        if m.addr in self.clients:
            self.clients.deleteClient(m.addr)
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
        self.pop(addr)