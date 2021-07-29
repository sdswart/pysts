from .. import components
from ..components.base import *
from ..config import * #METRICS_PATH
import pandas as pd
import numpy as np
from ..utils import open_spectra, remove_baseline

class NewPage(components.Component):
    def __init__(self):
        self.layout=self.get_layout()
        self.set_callbacks()
    def get_layout(self):
        self.dropdown=components.Dropdown(multi=True,options=None,update_value_on_change=True,keep_old_values=True,value=None)
        self.uploader=components.Uploader(type='area',multi=True,allowed_extensions=SPECTRA_EXTENSIONS,upload_to=SPECTRA_PATH,dropdown=self.dropdown,keep_old_uploads=True)
        self.info=html.Div()

        return html.Div(['Example Div',self.uploader,self.dropdown,self.info])

    def set_callbacks(self):
        @self([self.info.children],[self.dropdown.value])
        def fcn(paths):
            msg=''
            for path in paths:
                spectra,wl=open_spectra(path)
                spectra_nb,background=remove_baseline(spectra)
                msg+=str(wl)
            return msg
