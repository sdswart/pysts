import sys
import threading, time
from datetime import timedelta
from .listener import Listener

class Job(threading.Thread):
    def __init__(self, listener, interval, execute, *args, **kwargs):
        threading.Thread.__init__(self)
        self.listener=listener
        self.daemon = False
        self.stopped = threading.Event()
        self.interval = interval
        self.execute = execute
        self.args = args
        self.kwargs = kwargs

    def stop(self):
        self.stopped.set()
        self.join()
    def start(self,block=False):
        self.daemon = not block
        super().start()
    def run(self):
        while not self.stopped.wait(self.interval.total_seconds()):
            res=self.execute(*self.args, **self.kwargs)
            if res is False:
                self.stop()

class Timeloop(Listener):
    @Listener.event
    def interval(self,func,seconds,*args,**kwargs):
        return Job(self,timedelta(seconds=seconds),execute=func,*args,**kwargs)

if __name__ == "__main__":
    tl = Timeloop()
    COUNTER=0
    @tl.interval(seconds=2)
    def sample_job_every_2s():
        global COUNTER
        COUNTER+=1
        print("{}: 2s job current time : {}".format(COUNTER, time.ctime()))
    tl.start(block=True)
