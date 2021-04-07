from .base import *
from .base_component import Component, Item

class Tabs(Component):
    def __init__(self,contents,active_tab=None):
        labels=list(contents.keys())
        if active_tab is None or active_tab not in labels:
            active_tab=labels[0]
        self.tabs=dbc.Tabs([dbc.Tab(label=label, tab_id=label, style={'cursor': 'pointer'}) for label in labels],
                card=True,active_tab=active_tab)

        to_item = lambda label,content: content if isinstance(content,Item) else Item(self,label,content)
        item_contents=[to_item(f'content-{label}',html.Div(content,style={'display': 'block' if label==labels[0] else 'none'})) for label,content in contents.items()]
        self.layout = html.Div(
                        dbc.Card([
                            dbc.CardHeader(self.tabs),
                            dbc.CardBody(item_contents)
                        ,])
                    ,className='pt-3')

        self.properties={'active_tab':self.tabs.active_tab}

        @self([x.style for x in item_contents],self.active_tab)
        def change_content(active_tab):
            return [{'display': 'block' if label==active_tab else 'none'} for label in labels]
