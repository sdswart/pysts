from .base import *
from .base_component import Component
from .utils import get_dynamic_values

class Alert(Component):
    def __init__(self,duration=5):
        self.num_alerts=0
        self.alert=dbc.Alert  #(children='',color='primary')
        self.remove_alert=html.A  #('X',className='border')
        #self.interval=dcc.Interval(interval=duration * 1000, n_intervals=0)
        #dbc.Collapse([self.alert,self.interval],is_open=False)

        self.layout=html.Div()

        self.properties={'alerts':self.layout.children}

        '''@self([self.layout.is_open,self.interval.disabled],self.msg)
        def show_hide_msg(msg):
            return (False,True) if msg is None or msg =='' else (True,False)'''

        @self(self.layout.children,self.remove_alert.n_clicks,self.layout.children)
        def reset_msg(n_intervals,alerts):
            index_vals=get_dynamic_values(self.remove_alert.id[0]['type'])
            index=[index for index,val in index_vals.items() if val>0][0]
            return ([x for x in alerts if x['props']['id']['index']!=index],)

    def create(self,msg,color='primary'):
        self.num_alerts+=1
        return self.alert(children=dbc.Row([msg,self.remove_alert('X',index=str(self.num_alerts),href='#',className=f"btn btn-sm btn-{color}")],justify="between"),index=str(self.num_alerts),color=color)
