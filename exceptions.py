class MqttsnError(RuntimeError):
    @classmethod
    def raiseIfReturnCode(cls, returnCode):
        if returnCode == 0:
            return None
        for cls in cls.__subclasses__():
            if cls.returnCode == returnCode:
                raise cls()
        raise ValueError("Unknown returnCode 0x{:02x}".format(returnCode))

class MqttsnErrorCongestion(MqttsnError):
    returnCode = 0x1

class MqttsnErrorInvalidTopic(MqttsnError):
    returnCode = 0x2

class MqttsnErrorNotSupported(MqttsnError):
    returnCode = 0x3