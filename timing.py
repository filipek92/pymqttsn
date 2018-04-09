#!/usr/bin/env python3

from threading import Thread, Condition

import sched
import logging
logger = logging.getLogger(__name__)

class Scheduler(sched.scheduler):    
    def __repr__(self):
        return "{}(len(queue)={})".format(
                self.__class__.__name__,
                len(self.queue)
            )       

class ThreadScheduler(Scheduler):
    """docstring for MyScheduler"""
    def __init__(self, allow_not_running=False, autostart=True):
        Scheduler.__init__(self)
        self._condition = Condition()
        self.thread = None
        self.allow_not_running = allow_not_running
        if autostart:
            self.start()

    def run(self):
        while True:
            try:
                Scheduler.run(self)
                with self._condition:
                    self._condition.wait()
            except StopIteration:
                break
            except Exception as e:
                logger.exception(e)
        self.thread = None

    def enterabs(self, *args, **kwargs):
        if not self.is_active and not self.allow_not_running:
            raise RuntimeError("Scheduler is not running")
        with self._condition:
            self._condition.notify()
        return Scheduler.enterabs(self, *args, **kwargs)

    def start(self):
        if self.thread is None:
            self.thread = Thread(target=self.run, daemon=True, name="Scheduler thread")
            self.thread.start()

    @property
    def is_active(self):
        return self.thread.is_alive() if self.thread else False

    def _stop_raise(self):
        raise StopIteration()

    def stop(self):
        self.enter(-1, 0, self._stop_raise)
        self.thread = None

    def __repr__(self):
        return "{}(active={}, len(queue)={})".format(
                self.__class__.__name__,
                self.is_active,
                len(self.queue)
            )

if __name__ == '__main__':
    s = Scheduler()

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