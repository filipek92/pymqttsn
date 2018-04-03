from .messages import *
from .exceptions import MqttsnError
from .tools import TopicTable

def onMessage(m):
    print("New data on topic '{}': {}".format(m.topic, m.data))

class Client(object):
	def __init__(self, clientId):
		self.clientId = clientId
		self.clean = True
		self.state = "Disconnected"
		self.msgId = 0
		self.topicTable = TopicTable()
		self.onMessage = onMessage

	def _on_message(self, m):
		if self.onMessage:
			self.onMessage(m)
		else:
			print(m)

	def _write(self, m):
		print("Write:", m)
		return self.write_packet(bytes(m))

	def _read(self, waitfor=None, msgId=None):
		if waitfor is None:
			data, addr = self.read_packet()
			m = Message.fromBinary(data)
			print("Read:", m)
		else:
			while True:
				data, addr = self.read_packet()
				m = Message.fromBinary(data)
				print("Read:", m)
				if (type(m) is waitfor) and ((msgId is None) or (msgId == m.msgId)):
					break
				else:
					self.handle_message(m)
		return m

	def handle_message(self, msg):
		if type(msg) == MessagePublish:
			msg.topic = self.topicTable.getTopic(msg.topicId)
			self._on_message(msg)

		if type(msg) == MessageRegister:
			self.topicTable.add(topic=msg.topicName, id=msg.topicId)
			print("New topic registered", msg.topicName, msg.topicId)

	def connect(self, will=False, clean=None):
		if clean is None:
			clean = self.clean
		s = MessageConnect(self.clientId, will=will, clean=clean, duration=0)
		self._write(s)
		r = self._read(waitfor=MessageConnAck)
		MqttsnError.raiseIfReturnCode(r.returnCode)
		self.state = "Connected"

	def register(self, topic):
		s = MessageRegister(topicName=topic, msgId=self.msgId)
		self._write(s)
		r = self._read()
		assert(type(r) == MessageRegAck)
		assert(s.msgId == r.msgId)
		MqttsnError.raiseIfReturnCode(r.returnCode)
		self.msgId += 1
		self.topicTable.add(topic, r.topicId)
		return r.topicId

	def publish(self, topic, data, retain=False, qos=0):
		if type(topic) == int:
			topicId = topic
		elif type(topic) == str:
			topicId = self.topicTable.getTopicId(topic)
			if topicId is None:
				topicId = self.register(topic)
		else:
			raise TypeError("Topic must be str or int")
		s = MessagePublish(msgId=self.msgId, topicId=topicId, data=data, retain=retain, qos=qos)
		self._write(s)
		if qos !=0:
			r = self._read(waitfor=MessagePubAck, msgId=s.msgId)
			self.msgId += 1
			MqttsnError.raiseIfReturnCode(r.returnCode)

	def subscribe(self, topic, qos=0):
		s = MessageSubscribe(topic=topic, qos=qos, topicType=0, msgId=self.msgId)
		self._write(s)
		r = self._read(waitfor=MessageSubAck, msgId=s.msgId)
		self.msgId += 1
		return r.topicId

	def unsubscribe(self, topic):
		s = MessageUnsubscribe(topic=topic, topicType=0, msgId=self.msgId)
		self._write(s)
		r = self._read(waitfor=MessageUnsubAck, msgId=s.msgId)
		self.msgId += 1
		return None



