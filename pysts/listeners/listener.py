import threading, signal, time

from pysts.utils.utils import create_logger, append_fcns


class ServiceExit(Exception):
    """
    Custom exception which is used to trigger the clean exit
    of all running threads and the main program.
    """
    pass

def service_shutdown(signum, frame):
    raise ServiceExit

class Listener(object):
    """ Listerner class to run functions on events
        A listener may have multiple parallel jobs
        At a minimum, the listener should add one job if a callback is registered
        Each job may have multiple events to which callbacks may be allocated through the @self._event decorator
        E.g.,
            class Mouse(Listener):
                @Listener.event
                def on_click(self,x, y, button, pressed):
                    print(f"{'Pressed' if pressed else 'Released'} button {button}")
            mouse=Mouse()
            @mouse.on_click
            def my_func(x,y, button, pressed):
                print(f"it was at position ({x},{y})")

            class Timeloop(Listener):
                @Listener.event
                def interval(self,func,seconds,*args,**kwargs):
                    return Job(self,timedelta(seconds=seconds),execute=func,*args,**kwargs)

            tl=Timeloop()
            @tl.interval(seconds=2)
            def sample_job_every_2s():
                print("2s job current time : {}".format(COUNTER, time.ctime()))
    """
    contexts = threading.local()
    def __init__(self):
        self.logger = create_logger(self.__class__.__name__)
        type(self).add_instance(self)
    @classmethod
    def get_logger(cls):
        if not hasattr(cls.contexts, '_logger'):
            cls.contexts._logger = create_logger('listener')
        return cls.contexts._logger
    @classmethod
    def get_jobs(cls):
        if not hasattr(cls.contexts, '_jobs'):
            cls.contexts._jobs = {}
        return cls.contexts._jobs
    @classmethod
    def _add_job(cls,listener,job):
        clsjobs=cls.get_jobs()
        if listener not in clsjobs:
            clsjobs[listener]=[]
        if job not in clsjobs[listener]:
            clsjobs[listener].append(job)
    @classmethod
    def get_instances(cls):
        if not hasattr(cls.contexts, '_instances'):
            cls.contexts._instances=[]
        return cls.contexts._instances
    @classmethod
    def add_instance(cls,instance):
        instances=cls.get_instances()
        if instance not in instances:
            instances.append(instance)
    @classmethod
    def _block_main_thread(cls):
        signal.signal(signal.SIGTERM, service_shutdown)
        signal.signal(signal.SIGINT, service_shutdown)
        while cls.jobs_alive():
            try:
                time.sleep(1)
            except ServiceExit:
                cls._stop_jobs()
                break
    @classmethod
    def jobs_alive(cls):
        return len(cls.get_running_jobs())>0
    @classmethod
    def get_running_jobs(cls):
        return [job for listener,jobs in cls.get_jobs().items() for job in jobs if hasattr(job,'is_alive') and job.is_alive()]
    @classmethod
    def _get_listeners_jobs(cls,listeners=None):
        if listeners and type(listeners) not in [list,tuple]: listeners=[listeners]
        return [(listener,job) for listener,jobs in cls.get_jobs().items() if listeners is None or listener in listeners for job in jobs]
    @classmethod
    def _start_jobs(cls, block=False, listeners=None):
        for listener,j in cls._get_listeners_jobs(listeners):
            if callable(j):
                cls.get_jobs()[listener].remove(j)
                j=j()
                cls._add_job(listener,j)
            if 'block' in j.start.__code__.co_varnames:
                j.start(block)
            else:
                j.start()
            cls.get_logger().info("Started job {}".format(j))

        if block:
            cls._block_main_thread()

    @classmethod
    def _stop_jobs(cls, listeners=None):
        for listener,j in cls._get_listeners_jobs(listeners):
            cls.get_logger().info("Stopping job {}".format(j))
            j.stop()

    # Functions that may be used by instance
    @classmethod
    def event(cls,listener_func):
        def decorator(self,*event_args,**event_kwargs):
            no_args=False
            if len(event_args) == 1 and len(event_kwargs)==0 and callable(event_args[0]):
                callback_func=event_args[0]
                event_args=list(event_args)[1:]
                no_args=True

            def _decorator(callback_func):
                job=listener_func(self,callback_func,*event_args,**event_kwargs)
                if job:
                    cls._add_job(self,job)
                return callback_func
            if no_args:
                # No arguments, this is the decorator (manually set defaults)
                return _decorator(callback_func)
            else:
                # This is just returning the decorator
                return _decorator
        return decorator

    # Functions that may be overloaded
    def stop(self):
        self.logger.info(f"Stopping jobs for {self.__class__.__name__}.")
        type(self)._stop_jobs(listeners=self)
    def start(self, block=False):
        self.logger.info(f"Starting jobs for {self.__class__.__name__}...")
        type(self)._start_jobs(block=block, listeners=self)
