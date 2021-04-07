import base64
from io import BytesIO
import re

def create_wrap(debug_view):
    def wrap(control=None,observe=True,**wrapkwargs):
        def decor(fcn):
            @debug_view.capture(clear_output=True)
            def rtn(*args,**kwargs):
                print(f'Function: {fcn.__name__} with args {args} and kwargs {kwargs}')
                if control and len(args)==0 and len(kwargs)==0:
                    args=[control.value]
                else:
                    val=lambda x: x['new'] if isinstance(x,dict) and 'new' in x else x.value if hasattr(x,'value') else x
                    args=[val(x) for x in args]
                    kwargs={key:val(x) for key,x in kwargs.items()}
                return fcn(*args,**kwargs,**wrapkwargs)

            if control and observe:
                control.observe(rtn,names=['value'])
            return rtn
        return decor
    return wrap

def get_output_figure_widgets(output):
    res=[]
    for elem in output.outputs:
        if elem['output_type']=='display_data':
            if 'application/vnd.jupyter.widget-view+json' in elem['data'] and 'model_id' in elem['data']['application/vnd.jupyter.widget-view+json']:
                res.append(output.widgets[elem['data']['application/vnd.jupyter.widget-view+json']['model_id']])
    return res

def get_output_html(output):
    res=''
    removes=[r'<a.*?>|<\/a.*?>','<title>.+</title>']
    for elem in output.outputs:
        if elem['output_type']=='display_data':
            html=None
            if 'text/html' in elem['data']:
                res+=elem['data']['text/html']
            elif 'application/vnd.jupyter.widget-view+json' in elem['data'] and 'model_id' in elem['data']['application/vnd.jupyter.widget-view+json']:
                fig=output.widgets[elem['data']['application/vnd.jupyter.widget-view+json']['model_id']].figure
                tmpfile = BytesIO()
                fig.savefig(tmpfile, format='png')
                encoded = base64.b64encode(tmpfile.getvalue()).decode('utf-8')

                res+='<img src=\'data:image/png;base64,{}\'>'.format(encoded)
            elif 'text/plain' in elem['data']:
                res+=elem['data']['text/plain']

    for pattern in removes:
        res=re.sub(r'<a.*?>|<\/a.*?>','',res,re.I | re.M)

    return res
