from .. import components
from ..components.base import *
from ..config import * #METRICS_PATH
import pandas as pd
import numpy as np

class Overview(components.Component):
    def __init__(self):

        self.metrics=pd.read_csv(METRICS_PATH).astype({'Date':'datetime64'})

        units=self.metrics.SN.to_list()
        unit_options=[{'label': x, 'value': x} for x in units]
        display_options=[{'label': 'All Units', 'value': 'all'}]+unit_options
        highlight_options=[{'label': 'Latest Unit', 'value': 'latest'},{'label': 'None', 'value': 'none'}]+unit_options
        self.display=components.Dropdown(multi=True,label='Display',options=display_options,update_value_on_change=True,keep_old_values=True,value='all')
        self.highlight=components.Dropdown(multi=False,label='Highlight',options=highlight_options,update_value_on_change=True,keep_old_values=True,value='latest')

        def get_figs(highlights,metrics=None):
            if metrics is None: metrics=self.metrics
            if highlights is None: highlights=[]

            figs=[None]*4
            fig_cols=[('Coupon','Particle'),('Coupon SBR','Particle SBR'),('Coupon','Coupon Background'),('Particle','Particle Background')]
            renames={'Coupon':'Coupon 315.5nm Fe Intensity (au)',
                    'Coupon Background':'Coupon 315.5nm Fe Background',
                    'Coupon SBR':'Coupon 315.5nm Fe SBR',
                    'Particle':'Particle 315.5nm Fe Intensity (au)',
                    'Particle Background':'Particle 315.5nm Fe Background',
                    'Particle SBR':'Particle 315.5nm Fe SBR'}
            #metrics=metrics.rename(columns=renames)
            figs=[go.Figure(layout={'showlegend':False,'height':500,'xaxis_title':renames[fig_cols[i][0]],'yaxis_title':renames[fig_cols[i][1]]}) for i in range(len(fig_cols))]
            for sn in metrics.SN.unique():
                df=metrics[metrics.SN==sn]
                for i,fig in enumerate(figs):
                    #cur_fig=px.scatter(df, x=renames[fig_cols[i][0]], y=renames[fig_cols[i][1]], text='SN')
                    marker={'size':12,'line':dict(width=2,color='DarkSlateGrey'),'symbol':'diamond'} if sn in highlights else {}
                    fig_data=go.Scatter(x=df[fig_cols[i][0]], y=df[fig_cols[i][1]], mode='markers+text',text=df['SN'],textposition="top center",marker=marker)
                    fig.add_trace(fig_data)

            return tuple(figs)

        figs=get_figs(highlights=['latest'])

        self.ptc_cpn_graph=dcc.Graph(figure=figs[0])
        self.ptc_sbr_cpn_sbr_graph=dcc.Graph(figure=figs[1])
        self.cpn_b_cpn_graph=dcc.Graph(figure=figs[2])
        self.ptc_b_ptc_graph=dcc.Graph(figure=figs[3])

        self.combiner=html.Div('Not ready',style={'display':'None'})

        self.layout=html.Div([
                            self.combiner,
                            dbc.Row([dbc.Col(self.display),dbc.Col(self.highlight)]),
                            dcc.Loading([
                                dbc.Row([dbc.Col(self.ptc_cpn_graph),dbc.Col(self.ptc_sbr_cpn_sbr_graph)]),
                                dbc.Row([dbc.Col(self.cpn_b_cpn_graph),dbc.Col(self.ptc_b_ptc_graph)])
                            ])
                        ])

        self.properties={'info':self.combiner.children}

        @self([self.ptc_cpn_graph.figure,self.ptc_sbr_cpn_sbr_graph.figure,self.cpn_b_cpn_graph.figure,self.ptc_b_ptc_graph.figure],[self.info,self.display.value,self.highlight.value])
        def update_plots(info,display,highlights):
            metrics=self.metrics.astype({'Date':'datetime64'})
            if 'all' not in display:
                metrics=metrics[metrics.SN.isin(display)]

            if not isinstance(highlights,list): highlights=[highlights]
            clean_highlights=[]
            for highlight in highlights:
                if highlight=='latest' and metrics.shape[0]>0:
                    clean_highlights.append(metrics.sort_values(by='Date')['SN'].to_list()[-1])
                elif highlight=='none':
                    pass
                elif highlight in metrics.SN.to_list():
                    clean_highlights.append(highlight)

            return get_figs(highlights=clean_highlights,metrics=metrics)
