
from urllib.parse import quote as urlquote

from ..components import *


uploader=dcc.Upload(
        id='upload-data',
        children=html.Div([
            'Drag and Drop or ',
            html.A('Select Files')
        ]),
        style={
            'width': '100%',
            'height': '60px',
            'lineHeight': '60px',
            'borderWidth': '1px',
            'borderStyle': 'dashed',
            'borderRadius': '5px',
            'textAlign': 'center',
            'margin-top':'10px'
        },
        # Allow multiple files to be uploaded
        multiple=True
    )

list_group = html.Div(id='folder-list')

selected_spectra=html.Div([
    html.Label(['Selected Spectra: ']),
    dcc.Dropdown(id='selected_spectra',options=[],multi=True,value=[])
    ])
background=dcc.Checklist(id='remove_background',options=[{'label': 'Remove background', 'value': 'rm'}],value=[],className="p-2")

# Configuration and regions
configurations=[x for x in os.listdir(CONFIG_PATH) if x.endswith('.csv')]
default_config=None
if len(configurations)>0:
    default_config = 'default.csv' if 'default.csv' in configurations else configurations[0]
    regions=pd.read_csv(os.path.join(CONFIG_PATH,default_config))
else:
    regions=pd.DataFrame({},columns=CONFIG_COLUMNS)

config_upload=dcc.Upload(html.Button('Upload Configuration'),id='config_upload',multiple=False)
config_dropdown=dcc.Dropdown(id='config_dropdown',options=configurations,multi=False,value=default_config)
get_config_link(filename):
    if filename is None or filename=='':
        return {'children':'','href':'#'}
    return {'children':f'Download {filename}',
            'href':"/download/{}".format(urlquote(os.path.join(CONFIG_PATH,filename)))}
get_config_href = lambda filename: "#" if filename is None or filename=='' else "/download/{}".format(urlquote(os.path.join(CONFIG_PATH,filename)))
download_config = html.A(id='download_config',**get_config_href(default_config))
config_tools=dbc.Row([dbc.Col(config_dropdown,width=6),
                    dbc.Col(download_config)
                    dbc.Col(config_upload,width=2)])

#Results
fig=go.Figure(layout={'height':700,'xaxis_title':'Wavelength (µm)','yaxis_title':'Intensity'})
result_card = html.Div(dbc.Card(
    [
        dbc.CardHeader(
            dbc.Tabs(
                [
                    dbc.Tab(dbc.CardBody(dcc.Graph(id='chart', figure=fig)),label="Spectra Chart", tab_id="tab-chart",style={'cursor': 'pointer'}),
                    dbc.Tab(dbc.CardBody(html.Div(id='results_table')),label="Table Results", tab_id="tab-table",style={'cursor': 'pointer'}),
                ],
                id="card-tabs",
                card=True,
                active_tab="tab-chart",
            )
        ),
    ]
),className='pt-3')


# Main Layout
layout = html.Div([
        html.H1('ChipCHECK Spectra Amplitudes'),
        dbc.Row(dbc.Col(alert_collapse)),
        dbc.Row([
            dbc.Col(html.Div([dcc.Input(id='full_path_root',className='w-100',disabled=True),list_group,uploader,selected_spectra,background]),width=4),
            dbc.Col(html.Div([
                config_tools,
                dash_table.DataTable(
                    id='regions',
                    columns=[{"name": i, "id": i} for i in regions.columns],
                    data=regions.to_dict('records'),
                    editable=True
                )
            ]),width=7)
        ]),
        dbc.Row(dbc.Col(html.Div(result_card)))
    ],className='p-3')
