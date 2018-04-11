#!/usr/bin/env python3

from pymqttsn import TransparentGateway
from pymqttsn import Client
import socket
import click
import logging

UDP_DEFAULT_PORT = 5476

class UdpBase:
    def __init__(self, host="0.0.0.0", port=UDP_DEFAULT_PORT, bind=False):
        self.server_addr = (socket.gethostbyname(host), port)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if bind:
            self.socket.bind(self.server_addr)

    def read_packet(self):
        data, addr = self.socket.recvfrom(1024)
        return data, addr

    def write_packet(self, data, addr=None):
        if addr is None:
            return self.socket.sendto(data, self.server_addr)
        else:
            return self.socket.sendto(data, addr)

    def broadcast_packet(self, data, radius=None):
        return self.socket.sendto(data, ('255.255.255.255', self.server_addr[1]))

    def __del__(self):
        self.socket.close()

class UdpGateway(TransparentGateway, UdpBase):
    def __init__(self, udpPort, udpIp="0.0.0.0", mqttHost="localhost", mqttPort=1883):
        UdpBase.__init__(self, host=udpIp, port=udpPort, bind=True)
        TransparentGateway.__init__(self, host=mqttHost, port=mqttPort, enable_advertising=False)

    def read_packet(self):
        return UdpBase.read_packet(self)

    def write_packet(self, data, addr=None):
        return UdpBase.write_packet(self, data, addr)

    def broadcast_packet(self, data, radius=None):
        return UdpBase.write_packet(self, data, radius)

    def __repr__(self):
        return "{}(udpIp={}, udpPort={}, mqttHost={}, mqttPort={})".format(
                self.__class__.__name__,
                self.server_addr[0],
                self.server_addr[1],
                self.mqttHost,
                self.mqttPort
            )
     
class UdpClient(Client, UdpBase):
    T_RETRY = 2
    N_RETRY = 5
    
    def __init__(self, clientId, host="127.0.0.1", port=UDP_DEFAULT_PORT):
        UdpBase.__init__(self, port=port, host=host)
        Client.__init__(self, clientId=clientId)

    def __del__(self):
        Client.__del__(self)
        UdpBase.__del__(self)

    def read_packet(self):
        return UdpBase.read_packet(self)

    def write_packet(self, data, addr=None):
        return UdpBase.write_packet(self, data, addr)

    def write_packet_broadcast(self, data, radius):
        return UdpBase.write_packet(self, data, radius)

    def __repr__(self):
        return "{}(serverIp={}, serverPort={}, clientId={}, state={})".format(
                self.__class__.__name__,
                self.server_addr[0],
                self.server_addr[1],
                self.clientId,
                self.state
            ) 

def onMessage(m):
    print("New data on topic '{}': {}".format(m.topic, m.data))

@click.group()
def main():
    pass

@main.command()
@click.argument("client_id")
@click.option('--clean/--no-clean', default=False)
@click.option('-h', '--host', default="127.0.0.1")
@click.option('-p', '--port', default=UDP_DEFAULT_PORT)
@click.option('-r', '--register-callback', is_flag=True, default=False)
@click.option('-w', '--will', is_flag=True, default=False) 
@click.option("-k", '--keepalive', default=20)
def client(client_id, clean, host, port, register_callback, will, keepalive):
    c = UdpClient(client_id, host=host, port=port)
    c.connect(clean=clean, will=will, keepalive=keepalive)

    if register_callback:
        c.onMessage = onMessage

    import threading
    import code
    import readline
    import rlcompleter
    import os

    python_history = os.path.expanduser('~/.python_history')

    vars = globals()
    vars.update(locals())
    readline.set_completer(rlcompleter.Completer(vars).complete)
    readline.parse_and_bind("tab: complete")
    readline.read_history_file(python_history)
    code.interact(local=vars)
    readline.write_history_file(python_history)

@main.command()
@click.option("-p", "--port", default=UDP_DEFAULT_PORT)
@click.option("-i", "--interactive", is_flag=True)
@click.option("-h", "--host", default="localhost")
@click.option("-b", "--bind", default="0.0.0.0")
def gateway(port, interactive, host, bind):
    g = UdpGateway(port, mqttHost=host, udpIp=bind)
    
    logging.info(g)

    if interactive:
        import threading
        import code
        import readline
        import rlcompleter
        import os

        threading.Thread(target=g.loop).start();

        python_history = os.path.expanduser('~/.python_history')

        vars = globals()
        vars.update(locals())
        readline.set_completer(rlcompleter.Completer(vars).complete)
        readline.parse_and_bind("tab: complete")
        readline.read_history_file(python_history)
        code.interact(local=vars)
        readline.write_history_file(python_history)
        g.stop()
    else:
        g.loop()

@main.command()
def adv_test():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(('127.0.0.2', 5476))
    s.setblocking(0)


    while True:
        msg = s.recv(bufferSize) 
        print(msg)

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s:%(name)s:%(levelname)s: %(message)s",
        datefmt='%d.%m.%y %H:%M:%S'
        )
    main()