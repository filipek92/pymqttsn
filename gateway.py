from .messages import *
from .tools import TopicTable, SubscriptionList
from threading import Event

import logging

import paho.mqtt.client as mqtt

class TransparentGatewayClient(mqtt.Client):
    def __init__(self, clientId, addr, mqttHost, mqttPort, gateway):
        mqtt.Client.__init__(self)
        self.clientId = clientId
        self.addr = addr
        self.subscriptions = SubscriptionList()
        self.topicTable = TopicTable()
        self.nextTopicId = 1
        self.on_connect = self.mqttOnConnect
        self.on_message = self.mqttOnMessage
        self.loop_start()
        self.connect(mqttHost, mqttPort)
        self.gateway = gateway

    def mqttOnConnect(self, client, userdata, flags, rc):
        print("Connected to mqtt with result code "+str(rc))

    def mqttOnMessage(self, client, userdata, msg):
        try:
            print("Received message '" + str(msg.payload) + "' on topic '"
            + msg.topic + "' with QoS " + str(msg.qos))
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
            logging.exception(e)

    def __repr__(self):
        return "{}(clientId='{}', addr={})".format(
                self.__class__.__name__,
                self.clientId,
                self.addr,
                len(self.subscriptions)
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

    def onRegister(self, m):
        if not m.topicName in self.topicTable.topics:
            self.topicTable.add(m.topicName)
        return MessageRegAck(
            msgId=m.msgId,
            returnCode=0, 
            topicId=self.topicTable.getTopicId(m.topicName))

    def onPublish(self, m):
        topic = self.topicTable.getTopic(m.topicId)
        if topic is None:
            return MessagePubAck(msgId=m.msgId, topicId=m.topicId, returnCode=2)
        info = self.publish(topic=topic, payload=m.data, qos=m.flags.qos, retain=m.flags.retain)
        if m.flags.qos !=0:
            info.wait_for_publish()
            return MessagePubAck(msgId=m.msgId, topicId=m.topicId, returnCode=info.rc)

    def onSubscribe(self, m):
        #Unknown topic
        if not m.topic in self.topicTable.topics:
            self.topicTable.add(m.topic)
        result, mid = self.subscribe(m.topic, m.flags.qos)
        topicId = self.topicTable.getTopicId(m.topic)

        return MessageSubAck(msgId=m.msgId, topicId=topicId, returnCode=result, qos=0)

    def onUnsubscribe(self, m):
        result, mid = self.unsubscribe(m.topic)
        return MessageUnsubAck(msgId=m.msgId)

    def onPingReq(self, m):
        pass

class TransparentGateway:
    def __init__(self, host="localhost", port=1883):
        self.clients = {}
        self._stopEvent = Event()
        self.mqttHost = host
        self.mqttPort = port

    def dump(self):
        print("Client list:")
        for addr in self.clients:
            print("  {}: {}".format(addr, self.clients[addr]))

    def _read(self):
        data, addr = self.read_packet()
        msg = Message.fromBinary(data)
        msg.addr = addr
        print("Read:", msg)
        return msg

    def _write(self, msg):
        print("Write:", msg)
        packet = bytes(msg)
        return self.write_packet(packet, msg.addr)

    def loop(self):
        print("Gateway started")
        while not self._stopEvent.is_set():
            try:
                msg = self._read()
                if type(msg) is MessageConnect:
                    reply = self.onConnect(msg)
                elif type(msg) is MessageDisconnect:
                    reply = self.onDisconnect(msg)
                else:
                    client = self.clients.get(msg.addr)
                    reply = client.handle_packet(msg)
                if reply is not None:
                    reply.addr = msg.addr
                    self._write(reply)
            except KeyboardInterrupt:
                break
            except Exception as e:
                logging.exception(e)

    def stop(self):
        self._stopEvent.set()

    def onConnect(self, m):
        if not m.addr in self.clients:
            self.clients[m.addr] = TransparentGatewayClient(
                m.clientId, 
                m.addr, 
                self.mqttHost, 
                self.mqttPort, 
                gateway=self)
        else:
            raise KeyError("DuplicateConnect")
        return MessageConnAck(0)

    def onDisconnect(self, m):
        cl = self.clients.pop(m.addr)
        cl.disconnect()
