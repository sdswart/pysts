from pysts.utils.modules import Lazy_Module
from .listener import Listener
from pysts.utils.utils import append_fcns

pynput=Lazy_Module('pynput')

class InputListener(Listener):
    _callbacks=None
    def get_callbacks(self,*args,**kwargs):
        if not self._callbacks:
            self._callbacks={}
        return self._callbacks
    def add_func_to_dict(self,obj,key,func):
        if key in obj:
            func=append_fcns(obj[key])
        obj[key]=func

class Keyboard(InputListener):
    _hotkeys=None
    def get_hotkeys(self,*args,**kwargs):
        if not self._hotkeys:
            self._hotkeys={}
        return self._hotkeys
    def create_hotkey_listener(self):
        return pynput.keyboard.GlobalHotKeys(self.get_hotkeys())
    def create_listener(self):
        return pynput.keyboard.Listener(**self.get_callbacks())
    @Listener.event
    def on_hotkey(self,func,hotkey,*args,**kwargs):
        self.add_func_to_dict(self.get_hotkeys(),hotkey,func)
        return self.create_hotkey_listener
    @Listener.event
    def on_press(self,func,*args,**kwargs): #key
        self.add_func_to_dict(self.get_callbacks(),'on_press',func)
        return self.create_listener
    @Listener.event
    def on_release(self,func,*args,**kwargs): #key
        self.add_func_to_dict(self.get_callbacks(),'on_release',func)
        return self.create_listener

class Mouse(InputListener):
    def create_listener(self):
        return pynput.mouse.Listener(**self.get_callbacks())
    @Listener.event
    def on_move(self,func,*args,**kwargs): #x, y
        self.add_func_to_dict(self.get_callbacks(),'on_move',func)
        return self.create_listener
    @Listener.event
    def on_click(self,func,*args,**kwargs): #x, y, button, pressed
        self.add_func_to_dict(self.get_callbacks(),'on_click',func)
        return self.create_listener
    @Listener.event
    def on_scroll(self,func,*args,**kwargs): #x, y, dx, dy
        self.add_func_to_dict(self.get_callbacks(),'on_scroll',func)
        return self.create_listener

if __name__ == "__main__":
    keylistener = Keyboard()
    @keylistener.on_press
    def print_key_presses(key):
        vktext=f' with vk {key.vk}' if hasattr(key,'vk') else ''
        try:
            print('alphanumeric key {0} pressed{1}'.format(
                key.char,vktext))
        except AttributeError:
            print('special key {0} pressed{1}'.format(
                key,vktext))

    @keylistener.on_hotkey('<ctrl>+<alt>+q')
    def print_hotkey():
        print('HOTKEY: <ctrl>+<alt>+q pressed')


    mouselistener = Mouse()
    @mouselistener.on_click
    def print_click(x, y, button, pressed):
        print(f"Mouse button {button} {'pressed' if pressed else 'released'} at ({x}, {y})")

    Listener._start_jobs(block=True)
