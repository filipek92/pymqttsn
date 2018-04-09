class MqttsnError(RuntimeError):
    @classmethod
    def fromReturnCode(cls, returnCode):
        if returnCode == 0:
            return None
        for cls in cls.__subclasses__():
            if cls.returnCode == returnCode:
                return cls
        raise ValueError("Unknown returnCode 0x{:02x}".format(returnCode))

    @classmethod
    def raiseIfReturnCode(cls, returnCode):
        if returnCode == 0:
            return None
        exception = MqttsnError.fromReturnCode(returnCode)
        raise exception()

class MqttsnErrorCongestion(MqttsnError):
    returnCode = 0x1

class MqttsnErrorInvalidTopic(MqttsnError):
    returnCode = 0x2

class MqttsnErrorNotSupported(MqttsnError):
    returnCode = 0x3

class MqttsnErrorBrokerNotRespond(MqttsnError):
    returnCode = 0xfd

class MqttsnErrorNotConnected(MqttsnError):
    returnCode = 0xfe

class MqttsnErrorGatewayException(MqttsnError):
    returnCode = 0xff