from pysts.utils.modules import Lazy_Module

pynput=Lazy_Module('pynput')

class Keyboard(object):
    _controller=None
    @property
    def controller(self):
        if not self._controller:
            self._controller=pynput.keyboard.Controller()
        return self._controller

    @property
    def key(self):
        return pynput.keyboard.Key

    def press(self,key,release=False):
        self.controller.press(key)
        if release:
            self.release(key)

    def release(self,key):
        self.controller.release(key)

    def type(self,chars):
        self.controller.type(chars)

class Mouse(object):
    _controller=None
    @property
    def controller(self):
        if not self._controller:
            self._controller=pynput.mouse.Controller()
        return self._controller

    @property
    def position(self):
        return self.controller.position

    def click(self,left=True,button=None,double=True,num=None):
        num = num if num else (2 if double else 1)
        button=button if button else (Button.left if left else Button.right)
        self.controller.click(button, num)

    def press(self,left=True,button=None):
        button=button if button else (Button.left if left else Button.right)
        self.controller.press(button)

    def release(self,left=True,button=None):
        button=button if button else (Button.left if left else Button.right)
        self.controller.release(button)

    def move(self,x,y,relative=False):
        if relative:
            self.controller.move(x, y)
        else:
            self.controller.position = (x, y)

    def scroll(self,steps=1):
        self.controller.scroll(0, steps)
