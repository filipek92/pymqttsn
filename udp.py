from .gateway import TransparentGateway
from .client import Client
import socket

class UdpBase(socket.socket):
    def __init__(self, host="0.0.0.0", port=5476, bind=False):
        self.host = host
        self.port = port
        socket.socket.__init__(self, socket.AF_INET, socket.SOCK_DGRAM)
        if bind:
            self.bind((host, port))

    def read_packet(self):
        data, addr = self.recvfrom(1024)
        # print(data)
        return data, addr

    def write_packet(self, data, addr=None):
        # print(data)
        if addr is None:
            return self.sendto(data, (self.host, self.port))
        else:
            return self.sendto(data, addr)

class UdpGateway(TransparentGateway, UdpBase):
    def __init__(self, port, mqttHost="localhost", mqttPort=1883):
        UdpBase.__init__(self, port=port, bind=True)
        TransparentGateway.__init__(self, host=mqttHost, port=mqttPort)
     
class UdpClient(Client, UdpBase):
    def __init__(self, clientId, host, port):
        UdpBase.__init__(self, port=port, host=host)
        Client.__init__(self, clientId=clientId)