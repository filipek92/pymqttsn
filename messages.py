#!/usr/bin/env python3

from struct import pack, unpack
from itertools import chain

def getTypeId(type):
    return {
        # ADVERTISE:        0x00,
        # SEARCHGW:     0x01,
        # GWINFO:           0x02,
        MessageConnect:     0x04,
        MessageConnAck:     0x05,
        # WILLTOPICREQ: 0x06,
        # WILLTOPICREQ: 0x07,
        # WILLMSGREQ:       0x08,
        # WILLMSG:      0x09,
        MessageRegister:     0x0A,
        MessageRegAck:       0x0B,
        MessagePublish:      0x0C,
        MessagePubAck:           0x0D,
        # PUBCOMP:      0x0E,
        # PUBREC:           0x0F,
        # PUBREL:           0x10,
        MessageSubscribe:        0x12,
        MessageSubAck:           0x13,
        MessageUnsubscribe:  0x14,
        MessageUnsubAck:     0x15,
        MessagePingReq:      0x16,
        MessagePingResp:     0x17,
        MessageDisconnect:       0x18,
        # WILLTOPICUPD: 0x1A,
        # WILLTOPICRESP:    0x1B,
        # WILLMSGUPD:       0x1C,
        # WILLMSGRESP:  0x1D,
    }.get(type)

class Message:
    nextMessageId = 1;

    def __init__(self):
        if self.__class__ == Message:
            raise RuntimeError("Cannot create MQTTSN.Message directly")
        else:
            raise NotImplementedError("Subclasses of MQTTSN.Message must implement __init__")

    def getType(self):
        return self.__class__

    def isType(self, type):
        return type == self.__class__

    def __bytes__(self):
        b = self.pack()
        l = len(b)
        if l <= 253:
            return pack("=BB", l+2, self.typeId) + b
        else:
            return pack("=BHB", 1, l+4, self.typeId) + b

    @staticmethod
    def newMessageId():
        Message.nextMessageId += 1
        return Message.nextMessageId

    @staticmethod
    def fromBinary(binary):
        l = binary[0]
        data = binary[1:]
        if l == 1:
            l, = unpack("=H", binary[1:3])
            data = binary[3:]
        assert(len(binary) == l)
        msg_cls = Message.fromTypeId(data[0])
        return msg_cls.unpack(data[1:])

    @classmethod
    def listTypes(cls):
        c = ((x,) if hasattr(x, "typeId") else x.listTypes() for x in cls.__subclasses__())
        return chain.from_iterable(c)

    @staticmethod
    def listTypeIdsHex():
        return set(hex(x.typeId) for x in Message.listTypes())

    @staticmethod
    def listTypeIds():
        return set(x.typeId for x in Message.listTypes())

    @staticmethod
    def fromTypeId(typeId):
        for cls in Message.listTypes():
            if cls.typeId == typeId:
                return cls
        raise ValueError("Unknown message type")

class MessageReq(Message):
    pass

class MessageAck(Message):
    pass

class Flags:
    def __init__(self, value=None, *, dup=False, qos=0, retain=False, will=False, clean=False, topicType=0):
        if value is None:
            self.value = 0
            self.setBit(0x80, dup)
            self.value |= (qos & 0x03) << 5
            self.setBit(0x10, retain)
            self.setBit(0x08, will)
            self.setBit(0x04, clean)
            self.value |= topicType & 0x03
        else:
            self.value = value

    def setBit(self, mask, value=True):
        if value:
            self.value |= mask
        else:
            self.value &= ~mask

    def checkMask(self, mask):
        return bool(self.value & mask)

    @property
    def dup(self):
        return self.checkMask(0x80)

    @dup.setter
    def dup(self, value):
        self.setBit(0x80, value)

    @property
    def qos(self):
        v = (self.value >> 5) & 0x03
        if v == 3:
            v = -1
        return v

    @qos.setter
    def qos(self, value):
        self.setBit(0x60, False)
        self.value |= (value & 0x03) << 5

    @property
    def retain(self):
        return self.checkMask(0x10)

    @retain.setter
    def retain(self, value):
        self.setBit(0x10, value)

    @property
    def will(self):
        return self.checkMask(0x08)

    @will.setter
    def will(self, value):
        self.setBit(0x08, value)

    @property
    def clean(self):
        return self.checkMask(0x04)

    @clean.setter
    def clean(self, value):
        self.setBit(0x04, value)

    @property
    def topicType(self):
        return self.value & 0x03

    @topicType.setter
    def topicType(self, value):
        self.setBit(0x03, False)
        self.value |= topicType & 0x03

    def __int__(self):
        return self.value

    def __repr__(self):
        return "{}(value={}, dup={}, qos={}, retain={}, will={}, clean={}, topicType={})".format(
            self.__class__.__name__,
            self.value,
            self.dup,
            self.qos,
            self.retain,
            self.will,
            self.clean,
            self.topicType
            )

class MessageConnect(MessageReq):
    typeId = 0x04

    def __init__(self, clientId, will=False, clean=True, protocol=0x01, duration=0):
        self.clientId = clientId
        self.flags = Flags(will=will, clean=clean)
        self.protocol = protocol
        self.duration = duration

    def pack(self):
        return pack("=BBH",
            int(self.flags),
            self.protocol,
            self.duration
            ) + self.clientId.encode("utf-8")

    @classmethod
    def unpack(cls, binary):
        client_id = binary[4:].decode("utf-8")
        m = cls(client_id)
        m.flags = Flags(binary[0])
        m.protocol, m.duration = unpack("=xBH", binary[:4])
         
        return m

    def __repr__(self):
        return "{}(clientId='{}', will={}, clean={}, protocol=0x{:02x}, duration={})".format(
                self.__class__.__name__,
                self.clientId,
                self.flags.will,
                self.flags.clean,
                self.protocol,
                self.duration,
            )

class MessageConnAck(MessageAck):
    typeId = 0x05

    def __init__(self, returnCode):
        self.returnCode = returnCode

    def pack(self):
        return bytes((self.returnCode,))

    @classmethod
    def unpack(cls, binary):
        return cls(binary[0])

    def __repr__(self):
        return "{}(returnCode={})".format(self.__class__.__name__, self.returnCode)

class MessageRegister(MessageReq):
    typeId = 0x0A

    def __init__(self, *, topicId=0x0000, msgId=None, topicName=None):
        if msgId is None:
            msgId = self.newMessageId()
        self.topicId = topicId
        self.msgId = msgId & 0xFFFF
        self.topicName = topicName

    def pack(self):
        return pack("=HH", self.topicId, self.msgId & 0xFFFF) + self.topicName.encode('utf8')

    @classmethod
    def unpack(cls, binary):
        m = cls(msgId=0)
        m.topicId, m.msgId = unpack("=HH", binary[:4])
        m.topicName = binary[4:].decode('utf-8')
        return m

    def __repr__(self):
        return "{}(topicId={}, msgId={}, topicName='{}')".format(
            self.__class__.__name__,
            self.topicId,
            self.msgId,
            self.topicName)

class MessageRegAck(MessageAck):
    typeId = 0x0B

    def __init__(self, *, msgId, topicId=0x0000, returnCode=0):
        self.topicId = topicId
        self.msgId = msgId & 0xFFFF
        self.returnCode = returnCode

    def pack(self):
        return pack("=HHB", self.topicId, self.msgId & 0xFFFF, self.returnCode)

    @classmethod
    def unpack(cls, binary):
        m = cls(msgId=0)
        m.topicId, m.msgId, m.returnCode = unpack("=HHB", binary)
        return m

    def __repr__(self):
        return "{}(topicId={}, msgId={}, returnCode='{}')".format(
            self.__class__.__name__,
            self.topicId,
            self.msgId,
            self.returnCode)

class MessagePublish(MessageReq):
    typeId = 0x0C

    def __init__(self, *, dup=False, qos=0, retain=False, topicType=0, topicId=0, msgId=None, data=None):
        if msgId is None:
            msgId = self.newMessageId()
        self.flags = Flags(dup=dup, qos=qos, retain=retain, topicType=topicType)
        self.topicId = topicId
        self.msgId = msgId & 0xFFFF
        if data is None:
            data = bytes()
        self.data =data

    @property
    def dup(self):
        return self.flags.dup

    @property
    def qos(self):
        return self.flags.qos

    @property
    def retain(self):
        return self.flags.retain

    @property
    def topicType(self):
        return self.flags.topicType 

    def pack(self):
        return pack("=BHH", int(self.flags), self.topicId, self.msgId & 0xFFFF)+self.data

    @classmethod
    def unpack(cls, binary):
        m = cls(msgId=0)
        m.topicId, m.msgId = unpack("=xHH", binary[:5])
        m.flags = Flags(binary[0])
        m.data = binary[5:]
        return m

    def __repr__(self):
        return "{}(dup={}, qos={}, retain={}, topicType={}, topicId={}, msgId={}, len(data)={})".format(
            self.__class__.__name__,
            self.flags.dup,
            self.flags.qos,
            self.flags.retain,
            self.flags.topicType,
            self.topicId,
            self.msgId,
            len(self.data))

class MessagePubAck(MessageAck):
    typeId = 0x0D

    def __init__(self, *, msgId, topicId=0, returnCode=0):
        self.topicId = topicId
        self.msgId = msgId & 0xFFFF
        self.returnCode = returnCode

    def pack(self):
        return pack("=HHB", self.topicId, self.msgId & 0xFFFF, self.returnCode)

    @classmethod
    def unpack(cls, binary):
        m = cls(msgId=0)
        m.topicId, m.msgId, m.returnCode = unpack("=HHB", binary)
        return m

    def __repr__(self):
        return "{}(topicId={}, msgId={}, returnCode={})".format(
            self.__class__.__name__,
            self.topicId,
            self.msgId,
            self.returnCode)

class MessageSubscribe(MessageReq):
    typeId = 0x12

    def __init__(self, *, qos=0, topicType=0, msgId=None, topic=None):
        if msgId is None:
            msgId = self.newMessageId()
        self.flags = Flags(qos=qos, topicType=topicType)
        self.msgId = msgId & 0xFFFF          
        if type(topic) is int:
            self.topic = topic
            assert(topicType != 0)
        elif type(topic) is str:
            self.topicType = 0
            self.topic = topic
        elif topic is None:
            self.topicType = 1;
            self.topic = 0
        else:
            raise ValueError('Topic must be int or string')

    def pack(self):
        if self.flags.topicType == 0:
            return pack("=BH",
                int(self.flags),
                self.msgId & 0xFFFF) + self.topic.encode('utf-8')
        else:
            return pack("=BHH", int(self.flags), self.msgId, self.topic)

    @classmethod
    def unpack(cls, binary):
        m = cls()
        m.flags = Flags(binary[0])
        if m.flags.topicType == 0:
            m.msgId, = unpack("=xH", binary[:3])
            m.topic = binary[3:].decode("utf-8")
        else:
            m.msgId, m.topicId = unpack("=xHH", binary[:5])
        return m

    def __repr__(self):
        return "{}(qos={}, topicType={}, msgId={}, topic={})".format(
            self.__class__.__name__,
            self.flags.qos,
            self.flags.topicType,
            self.msgId,
            self.topic)

class MessageSubAck(MessageAck):
    typeId = 0x13

    def __init__(self, *, msgId, qos=0, topicId=0, returnCode=0):
        self.flags = Flags(qos=qos)
        self.topicId = topicId
        self.msgId = msgId & 0xFFFF
        self.returnCode = returnCode

    def pack(self):
        return pack("=BHHB", int(self.flags), self.topicId, self.msgId & 0xFFFF, self.returnCode)

    @classmethod
    def unpack(cls, binary):
        m = cls(msgId=0)
        m.topicId, m.msgId, m.returnCode = unpack("=xHHB", binary)
        m.flags = Flags(binary[0])
        return m

    def __repr__(self):
        return "{}(qos={}, topicId={}, msgId={}, returnCode={})".format(
            self.__class__.__name__,
            self.flags.qos,
            self.topicId,
            self.msgId,
            self.returnCode)

class MessageUnsubscribe(MessageReq):
    typeId = 0x14

    def __init__(self, *, msgId=None, topicType=1, topic=None):
        if msgId is None:
            msgId = self.newMessageId()
        self.flags = Flags(topicType=topicType)
        self.msgId = msgId & 0xFFFF          
        if type(topic) is int:
            assert(topicType != 0)
            self.topic = topic
        elif type(topic) is str:
            assert(topicType == 0)
            self.topic = topic
        elif topic is None:
            self.topic = 0
        else:
            raise ValueError('Topic must be int or string')

    def pack(self):
        if self.flags.topicType == 0:
            return pack("=BH",
                int(self.flags),
                self.msgId & 0xFFFF) + self.topic.encode('utf-8')
        else:
            return pack("=BHH",
                int(self.flags),
                self.msgId & 0xFFFF,
                self.topic)

    @classmethod
    def unpack(cls, binary):
        m = cls()
        m.flags = Flags(binary[0])
        if m.flags.topicType == 0:
            m.msgId, = unpack("=xH", binary[:3])
            m.topic = binary[3:].decode("utf-8")
        else:
            m.msgId, m.topicId = unpack("=xHH", binary[:5])
        return m

    def __repr__(self):
        return "{}(topicType={}, msgId={}, topic={})".format(
            self.__class__.__name__,
            self.flags.topicType,
            self.msgId,
            self.topic)

class MessageUnsubAck(MessageAck):
    typeId = 0x15

    def __init__(self, *, msgId=0):
        self.msgId = msgId & 0xFFFF

    def pack(self):
        return pack("=H", self.msgId & 0xFFFF)

    @classmethod
    def unpack(cls, binary):
        m = cls()
        m.msgId, = unpack("=H", binary)
        return m

    def __repr__(self):
        return "{}(msgId={})".format(
            self.__class__.__name__,
            self.msgId)

class MessagePingReq(MessageReq):
    typeId = 0x16

    def __init__(self, clientId=None):
        if clientId is None:
            clientId = bytes()
        self.clientId = clientId

    def pack(self):
        return self.clientId

    @classmethod
    def unpack(cls, binary):
        return cls(binary)

    def __repr__(self):
        if len(self.clientId) == 0:
            return "{}()".format(self.__class__.__name__)
        else:
            return "{}(clientId={})".format(
                self.__class__.__name__,
                self.clientId)

class MessagePingResp(MessageAck):
    typeId = 0x17

    def __init__(self):
        pass

    def pack(self):
        return bytes()

    @classmethod
    def unpack(cls, binary):
        return cls()

    def __repr__(self):
        return "{}()".format(self.__class__.__name__)

class MessageDisconnect(MessageReq):
    typeId = 0x18

    def __init__(self, duration=None):
        self.duration = duration

    def pack(self):
        if self.duration is None:
            return bytes()
        else:
            return pack("=H", self.duration)

    @classmethod
    def unpack(cls, binary):
        m = cls()
        if len(binary) > 0:
            m.duration, = unpack("=H", binary)  
        return m

    def __repr__(self):
        if self.duration is None:
            return "{}()".format(self.__class__.__name__)
        else:
            return "{}(duration='{}')".format(self.__class__.__name__, self.duration)        
