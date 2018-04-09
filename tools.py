from bidict import bidict
from paho.mqtt.client import topic_matches_sub
from collections import namedtuple
from threading import Event

class TopicTable(bidict):
    def __init__(self, *args, **kwargs):
        bidict.__init__(self, *args, **kwargs)
        self.nextTopicId = 0

    def newTopicId(self):
        self.nextTopicId += 1
        return self.nextTopicId

    def dump(self):
        print(str(self))

    def __str__(self):
        return "TopicId map:\n"+"\n".join("  {} --> {}".format(x, self[x]) for x in self)

    def add(self, topic, id=None):
        if id is None:
            id = self.newTopicId()
        self[id] = topic
        return id

    def remove(self, x):
        try:
            del self[x]
        except KeyError:
            pass

        try:
            del self.inv[x]
        except KeyError:
            pass

    def getTopicId(self, topic):
        return self.inv.get(topic)
    
    def getTopic(self, id):
        return self.get(id)

    @property
    def topics(self):
        return self.inv

    @property
    def ids(self):
        return self

class WaitingKey(namedtuple('WaitingKey', ['address', 'type', 'msgId'])):
    def __repr__(self):
        if self.msgId is not None:
            return "{}(address={}, type={}, msgId={})".format(self.__class__.__name__, self.address, self.type.__name__, self.msgId)
        else:
            return "{}(address={}, type={})".format(self.__class__.__name__, self.address, self.type.__name__)


class WaitingEvent(Event):
    """docstring for Waiting key"""
    def __init__(self):
        Event.__init__(self)
        self.reply = None

    def setReply(self, reply):
        self.reply = reply
        self.set()

    def __repr__(self):
        return "{}(reply={}, is_set={})".format(self.__class__.__name__, self.reply, self.is_set())

class WaitingList(dict):
    def add_waiting(self, address, waitfor, msgId=None, retry=None):
        key = WaitingKey(address=address, type=waitfor, msgId=msgId)
        event = WaitingEvent()
        self[key] = event
        return event

    def get_waiting(self, address, type, msgId=None):
        key = WaitingKey(address=address, type=type, msgId=msgId)
        if key in self:
            return self.pop(key)
        return None        
        