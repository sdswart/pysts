import subprocess, sys, os, hashlib, re
from ipywidgets import widgets
from datetime import datetime
import base64
from io import BytesIO
from IPython.utils.io import capture_output
from IPython.display import display

def get_package(name,install=None):
    try:
        pkg=__import__(name)
    except:
        if install is None:
            install=name
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--trusted-host", "pypi.org", "--trusted-host", "files.pythonhosted.org", install])
        pkg=__import__(name)
    return pkg

get_filename=lambda path: os.path.splitext(os.path.split(path)[-1])[0]

def get_output_path(notebook_path,inputs,folder='Reports',extension='docx'):
    notebook_name=get_filename(notebook_path)
    input_text=''
    if isinstance(inputs,dict):
        process=lambda x: re.sub('\W+',' ', os.path.dirname(x) if os.path.isdir(x) else get_filename(x))[:10]
        input_data=[process(x) for x in list(inputs.values())[:2] if isinstance(x,str)]
        input_text='_'+'_'.join(input_data)
    report_name=f'{notebook_name}{input_text}_{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}.{extension}'
    return os.path.join(folder,report_name)


def get_hash(context):
    return hashlib.sha224(str(context).encode()).hexdigest()

def dict_to_environ(inputs):
    for key,val in inputs.items():
        if isinstance(key,str) and isinstance(val,str):
            os.environ[key]=val

def capture_code(code,scope=None):
    if scope is None:
        scope={}

    #import pdb;pdb.set_trace()
    print('---------------------------CODE----------------------------------------')
    print(code)
    with capture_output() as capture:

        exec(compile(code,"na","exec"),scope,scope)

    return capture,scope

def run_code_in_shell(code):
    from IPython.utils.io import capture_output
    run_code=("python -c 'from IPython.core.interactiveshell import InteractiveShell\n"+
          "shell=InteractiveShell().instance()\n"+
          "from IPython.utils.io import capture_output\n"+
          "co=capture_output()\ncapture=co.__enter__()\n"+
          code+"\nco.__exit__(1,2,3)\n"+
          "for output in capture.outputs:\n"+
          "\tif \"text/html\" in output.data:\n"+
          "\t\tprint(output.data[\"text/html\"])'")
    proc = subprocess.Popen(run_code, shell=True,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return proc.communicate()[0].decode('utf-8')

    #Example ------------------------------
    import nbformat
    path='/home/jovyan/shared/BTR/Standard Report/Types/Snapshot.ipynb'
    notebook=nbformat.read(path, as_version=4)
    for i,cell in enumerate(notebook['cells']):
        if cell['cell_type']=='code':
            code=''.join([line for line in cell['source'].split('\n') if not line.startswith('%')])
            print(run_code(code))

def outputs_to_html(outputs,types=['text/html','application/vnd.jupyter.widget-view+json']):
    res=''
    removes=[r'<a.*?>|<\/a.*?>','<title>.+</title>']
    if not isinstance(outputs,list) and hasattr(outputs,'outputs'):
        outputs=outputs.outputs

    for output in outputs:
        if types is None or any([x in output.data for x in types]):
            if 'text/html' in output.data:
                res+=output.data['text/html']
            elif 'application/vnd.jupyter.widget-view+json' in output.data and 'model_id' in output.data['application/vnd.jupyter.widget-view+json']:
                model_id=output.data['application/vnd.jupyter.widget-view+json']['model_id']
                if model_id in widgets.Widget.widgets:
                    widget=widgets.Widget.widgets[model_id]
                    if hasattr(widget,'figure'):
                        fig=widget.figure
                        fig.tight_layout()
                        tmpfile = BytesIO()
                        fig.savefig(tmpfile, format='png')
                        encoded = base64.b64encode(tmpfile.getvalue()).decode('utf-8')
                        res+='<p><img src=\'data:image/png;base64,{}\'></p>'.format(encoded)
            elif 'image/png' in output.data:
                encoded = base64.b64encode(output.data['image/png']).decode('utf-8')
                res+='<p><img src=\'data:image/png;base64,{}\'></p>'.format(encoded)
            else:
                res+=output.data['text/plain']

    for pattern in removes:
        res=re.sub(r'<a.*?>|<\/a.*?>','',res,re.I | re.M)

    return res

def outputs_to_output(outputs,ipyoutput,types=['text/html','application/vnd.jupyter.widget-view+json']):
    if not isinstance(outputs,list) and hasattr(outputs,'outputs'):
        outputs=outputs.outputs
    with ipyoutput:
        for output in outputs:
            if types is None or not hasattr(output,'data') or any([x in output.data for x in types]):
                display(output)


def get_outputs_figure_widgets(outputs):
    res=[]
    if not isinstance(outputs,list) and hasattr(outputs,'outputs'):
        outputs=outputs.outputs
    for output in outputs:
        if 'application/vnd.jupyter.widget-view+json' in output.data and 'model_id' in output.data['application/vnd.jupyter.widget-view+json']:
            model_id=output.data['application/vnd.jupyter.widget-view+json']['model_id']
            if model_id in widgets.Widget.widgets:
                canvas=widgets.Widget.widgets[model_id]
                canvas.figure.tight_layout()
                canvas.header_visible = False
                canvas.footer_visible = False
                res.append(canvas)
    return res
