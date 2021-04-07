import tkinter as tk
import threading

from pysts.utils.utils import create_logger, append_fcns


class Component(object):
    """ tkinter component
    """
    contexts = threading.local()
    def __init__(self):
        self.logger = create_logger(self.__class__.__name__)
        type(self).add_instance(self)
    @classmethod
    def get_logger(cls):
        if not hasattr(cls.contexts, '_logger'):
            cls.contexts._logger = create_logger('component')
        return cls.contexts._logger
    @classmethod
    def get_callbacks(cls):
        if not hasattr(cls.contexts, '_callbacks'):
            cls.contexts._callbacks = []
        return cls.contexts._callbacks
    @classmethod
    def _add_callback(cls,callback):
        callbacks=cls.get_callbacks()
        callbacks.append(callback)
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

    # Functions that may be used by instance
    @classmethod
    def callback(cls,component_func):
        def decorator(self,*event_args,**event_kwargs):
            no_args=False
            if len(event_args) == 1 and len(event_kwargs)==0 and callable(event_args[0]):
                callback_func=event_args[0]
                event_args=list(event_args)[1:]
                no_args=True

            def _decorator(callback_func):
                callback=component_func(self,callback_func,*event_args,**event_kwargs)
                if callback:
                    cls._add_callback(self,callback)
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
