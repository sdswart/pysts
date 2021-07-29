import dash
from flask import Flask
import dash_bootstrap_components as dbc
import dash_html_components as html
import os
from flask import request

from .components.base import *
from . import components
from utils import open_browser

def create_app(main_component,pathname_prefix='/',config=None):
    server = Flask(__name__)
    SECRET_KEY=config.SECRET_KEY if config is not None else 'adsfyiu3S3!jE%$axbjhwa195sxc@S'
    server.secret_key=SECRET_KEY

    app = dash.Dash(server=server,
        url_base_pathname=pathname_prefix,
        external_stylesheets=[dbc.themes.BOOTSTRAP],
        suppress_callback_exceptions=True)
    app.scripts.config.serve_locally=True
    app.css.config.serve_locally=True

    #Download URL
    components.Download.register(app,prefix=pathname_prefix)

    # Create Dash Layout
    main_app=main_component(config) if callable(main_component) else main_component
    app.layout=main_app.generate_layout()
    components.generate_all_callbacks(app,verbose=0)
    return app

def start_app(main_component,pathname_prefix='/',debug=True, dev_tools_ui=True, dev_tools_props_check=True, host='0.0.0.0',port=5000,config=None):
    if config is None:
        from config import config
    dashapp=create_app(main_component,pathname_prefix='/',config=config)
    app=dashapp.server
    dashapp.run_server(debug=debug, dev_tools_ui=dev_tools_ui, dev_tools_props_check=dev_tools_props_check, host=host,port=port)