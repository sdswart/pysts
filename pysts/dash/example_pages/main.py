from .components.base import *
from . import components

class Main(components.Component):
    def layout(self):
        version=os.environ.get('VERSION','unknown')
        self.overview=Overview()
        self.edit=Edit(overview=self.overview)
        self.amplitudes=Amplitudes(main_app=self)
        self.newpage=NewPage()
        self.tabs=components.Tabs({
                'Overview':self.overview,
                'Edit':self.edit,
                'Amplitudes':self.amplitudes,
                'NewPage':self.newpage
            })
        self.full_layout=html.Div([
            html.H1('ChipCHECK Verification'+(' - DEVELOPMENT BUILD' if os.environ.get('GIT_BRANCH','none').endswith('dev') else '')),
            self.tabs,
            html.Div(html.P(f'V{version}' if version !='unknown' else ''),style={'border-top':'1px solid rgba(0,0,0,.125)','width':'100%'},className='pt-3')
            ],className='pt-3')

        return self.full_layout
