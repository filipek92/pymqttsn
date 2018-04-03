from .udp import UdpGateway
import click

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