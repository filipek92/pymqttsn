from pymqttsn.gateway import TransparentGateway
from pymqttsn.client import Client
import socket
import click

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

    def __del__(self):
        self.close()

class UdpGateway(TransparentGateway, UdpBase):
    def __init__(self, port, mqttHost="localhost", mqttPort=1883):
        UdpBase.__init__(self, port=port, bind=True)
        TransparentGateway.__init__(self, host=mqttHost, port=mqttPort)
     
class UdpClient(Client, UdpBase):
    def __init__(self, clientId, host, port):
        UdpBase.__init__(self, port=port, host=host)
        Client.__init__(self, clientId=clientId)

    def __del__(self):
        Client.__del__(self)
        UdpBase.__del__(self)

@click.command()
@click.option("-p", "--port", default=5476)
@click.option("-i", "--interactive", is_flag=True)
@click.option("-h", "--host", default="localhost")
def main(port, interactive, host):
    g = UdpGateway(port, mqttHost=host)
    print(g)

    if interactive:
        import threading
        import code
        import readline
        import rlcompleter
        threading.Thread(target=g.loop).start();

        vars = globals()
        vars.update(locals())
        readline.set_completer(rlcompleter.Completer(vars).complete)
        readline.parse_and_bind("tab: complete")

        code.interact(local=vars)
        g.stop()
    else:
        g.loop()

if __name__ == '__main__':
    main()