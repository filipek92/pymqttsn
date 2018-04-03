from bidict import bidict
from paho.mqtt.client import topic_matches_sub

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

class SubscriptionList(set):
    def forTopic(self, topic):
        return filter(lambda sub: self.matchTopic(topic, sub), self)

    @staticmethod
    def matchTopic(topic, subscription):
        return topic_matches_sub(subscription, topic)      