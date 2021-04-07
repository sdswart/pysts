import tkinter as tk

from .utils import Component

class FrameCol(object):


class Frame(tk.Frame,Component):
    _cols=None
    def __init__(self,cols=1,rows=1,*args,**kwargs):
        """ Initialize a frame with a grid layout
            The grid is defined by the inputs rows and cols
            Use indices to get a cell frame as [column][row]
            E.g.
                frame = Frame(2,2)
                cell_12 = frame[1][2]
            Inputs:
                rows,cols - int or [dict]
                    if integer - defines the number of equal spaced default sytle rows/cols
                    if a single/list of dictionaries - defines the properties of each row or col
                    E.g. rows=3, cols=3
                            or
                        rows=[{'weight':1, 'minsize':75},{'weight':1, 'minsize':100}]
        """
        super().__init__(*args,**kwargs)
        if isinstance(cols,int): cols=[{}]*cols
        if isinstance(rows,int): rows=[{}]*rows
        if type(rows) not in [list,tuple]:rows=[rows]
        if type(cols) not in [list,tuple]:cols=[cols]
        for i_row,row in enumerate(rows):
            self.columnconfigure(i_row, **col)
            for i_col,col in enumerate(cols):
                if i_row==0:
                    self.columnconfigure(i_col, **col)

    def get_border_effect(self,name):
        border_effects = {
            "flat": tk.FLAT,
            "sunken": tk.SUNKEN,
            "raised": tk.RAISED,
            "groove": tk.GROOVE,
            "ridge": tk.RIDGE,
        }
        return border_effects[name]

    def __getitem__(object,i_col):
        if self._cols is None
