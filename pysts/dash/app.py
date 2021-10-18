import dash_devices
import dash_bootstrap_components as dbc
import dash_html_components as html
import os
import posixpath
import quart
import secrets

from .components.base import *
from . import components
from .utils import open_browser
from .config import Config

def create_app(main_component,pathname_prefix='/',config=None,static_path=None,static_route='/static/',external_stylesheets=None,external_scripts=None):
    server = quart.Quart(__name__)
    SECRET_KEY=config.SECRET_KEY if config is not None else secrets.token_urlsafe(16)
    server.secret_key=SECRET_KEY
    if external_stylesheets is None:
        external_stylesheets=[]
    external_stylesheets.append(dbc.themes.BOOTSTRAP)

    app = dash_devices.Dash(server=server,
        url_base_pathname=pathname_prefix,
        external_stylesheets=external_stylesheets,
        external_scripts=external_scripts,
        suppress_callback_exceptions=True)
    app.scripts.config.serve_locally=True
    app.css.config.serve_locally=True

    #Download URL
    components.Download.register(app,prefix=pathname_prefix)

    # Create Dash Layout
    main_app=main_component if isinstance(main_component,components.Component) else main_component(config)
    app.layout=main_app.generate_layout()
    components.generate_all_callbacks(app,verbose=0)

    #Serve static files
    if static_path is not None:
        # for root, fols, files in os.walk(static_path):
        #     url_path = posixpath.join(static_route, *[x for x in os.path.relpath(root,static_path).split(os.sep) if not x.startswith('.')])
        @app.server.route('{}<path:file_path>/<filename>'.format(static_route))
        async def serve_static(file_path,filename):
            file_path=os.path.join(file_path,filename).replace('/',os.sep)
            fullpath=os.path.join(static_path,file_path)
            assert os.path.isfile(fullpath), f'The file {file_path} could not be found'
            path,file=os.path.split(fullpath)
            return await quart.send_from_directory(path,file)
    return app

def start_app(main_component=None,dashapp=None,pathname_prefix='/',debug=True, dev_tools_ui=True, dev_tools_props_check=True, host='0.0.0.0',port=5000,config=None,
            static_path=None,static_route='/static/',external_stylesheets=None,external_scripts=None):
    assert main_component is not None or dashapp is not None, 'main_component or dashapp is required!'
    if config is None:
        config=Config()
    if dashapp is None:
        dashapp=create_app(main_component,pathname_prefix='/',config=config,static_path=static_path,
                        static_route=static_route,external_stylesheets=external_stylesheets,external_scripts=external_scripts)
    app=dashapp.server
    dashapp.run_server(debug=debug, dev_tools_ui=dev_tools_ui, dev_tools_props_check=dev_tools_props_check, host=host,port=port)

def start_basic_server():
    #Start a basic dash server on port 5000
    class Main(components.Component):
        def layout(self):
            self.full_layout=html.Div('Test Dash server')
            return self.full_layout
    start_app(Main())
