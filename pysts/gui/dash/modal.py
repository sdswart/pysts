from .base import *
from .base_component import Component

class Modal(Component):
    def __init__(self,open_label='Open Modal',open_color='primary',header='Header',body='Modal Content',action_label='Ok',action_color='primary',show_cancel=True,action_fcn=None,small=False):

        self.open=html.A(open_label,href='#',className=f"ml-auto btn btn-{open_color}{' btn-sm' if small else ''}")
        self.modal_header=dbc.ModalHeader(header)
        self.modal_body=dbc.ModalBody(body)
        self.cancel=html.A('Cancel',href='#',className="ml-auto btn btn-secondary mr-3")
        self.action=html.A(action_label,href='#',className=f"ml-auto btn btn-{action_color}")
        self.layout = dbc.Modal([
                        self.modal_header,self.modal_body,
                        dbc.ModalFooter(dbc.Row(([self.cancel] if show_cancel else [])+[self.action],justify='end')),
                    ])

        self.properties={'is_open':self.layout.is_open,
                        'header':self.modal_header.children,
                        'body':self.modal_body.children}
        self.action_clicks=0
        @self(self.layout.is_open,[self.open.n_clicks,self.cancel.n_clicks,self.action.n_clicks],self.layout.is_open)
        def show_modal(open_click,cancel_click,action_click,is_open):
            if action_click!=self.action_clicks and action_fcn is not None:
                action_fcn()
                self.action_clicks=action_click
            if open_click or cancel_click or action_click:
                return not is_open
            return is_open
