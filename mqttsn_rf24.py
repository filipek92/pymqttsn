#!/usr/bin/env python3

from pymqttsn.gateway import TransparentGateway
from pymqttsn.client import Client
import click

from RF24 import *
from RF24Network import *
from RF24Mesh import *

class RF24Gateway(TransparentGateway):
    def __init__(self, nodeId=0, mqttHost="localhost", mqttPort=1883):
        TransparentGateway.__init__(self, host=mqttHost, port=mqttPort)

        # radio setup for RPi B Rev2: CS0=Pin 24
        self.radio = RF24(RPI_V2_GPIO_P1_15, RPI_V2_GPIO_P1_24, BCM2835_SPI_SPEED_8MHZ)
        self.network = RF24Network(self.radio)
        self.mesh = RF24Mesh(self.radio, self.network)

        self.mesh.setNodeID(nodeId)
        self.mesh.begin()
        self.radio.setPALevel(RF24_PA_MAX) # Power Amplifier
        self.radio.printDetails()

    def read_packet(self):
        self.mesh.update()
        self.mesh.DHCP()

        header, payload = self.network.read(10)
        if chr(header.type) == 'M':
            return payload, header.from_node

    def write_packet(self, data, addr=None):
        return self.mesh.write(data, ord('M'), addr)
        
@click.command()
@click.option("-i", "--interactive", is_flag=True)
@click.option("-h", "--host", default="localhost")
def main(interactive, host):
    g = RF24Gateway(mqttHost=host)
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