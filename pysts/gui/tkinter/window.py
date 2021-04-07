import tkinter as tk

class ServiceExit(Exception):
    """
    Custom exception which is used to trigger the clean exit
    of all running threads and the main program.
    """
    pass

def service_shutdown(signum, frame):
    raise ServiceExit

class Window(tk.Tk):
    def __init__(self,rows=1,cols=1,*args,**kwargs):
        super().__init__(*args,**kwargs)

    def stop(self):
        self.destroy()
    def start(self):
        self.mainloop()
