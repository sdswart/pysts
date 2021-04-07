import tkinter as tk

class App(tk.Frame):
    def __init__(self, master=None):
        super().__init__(master)
        self.pack()

    def run(self):
        root = tk.Tk()
        app = Application(master=root)
        app.mainloop()

if __name__ == '__main__':
    # create the application
    myapp = App()

    #
    # here are method calls to the window manager class
    #
    myapp.master.title("My Do-Nothing Application")
    myapp.master.maxsize(1000, 400)
    myapp.mainloop()
