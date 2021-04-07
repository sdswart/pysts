from nbconvert.filters.markdown_mistune import IPythonRenderer, MarkdownWithMath
from nbconvert.preprocessors import ExecutePreprocessor
import nbformat, os, subprocess, json
from IPython import get_ipython
import matplotlib.pyplot as plt
from IPython.display import display,HTML
from IPython.utils.capture import RichOutput

from .utils import *

def get_notebook(nb):
    if isinstance(nb,str) and os.path.isfile(nb):
        with open(nb) as f:
            nb = nbformat.read(f, as_version=4)
    return nb

def get_notebook_inputs(nb):
    nb=get_notebook(nb)
    res={}
    for cell in nb['cells']:
        if cell['cell_type']=='raw':
            lines=cell['source'].split('\n')
            if lines[0].lower().startswith('inputs'):
                for line in lines:
                    if '=' in line:
                        parts=line.split('=')
                        res[parts[0]]=json.loads(parts[1])
    return res


def run_notebook(path,inputs=None,timeout=3600,destination=None,allow_errors=False):
    nb=get_notebook(path)

    if isinstance(inputs,dict):
        dict_to_environ(inputs)

    ep = ExecutePreprocessor(timeout=3600, kernel_name='python3')
    ep.allow_errors = allow_errors

    notebook_dir=os.path.split(path)[0]
    path_dir='./' if notebook_dir== '' else (os.path.relpath(os.path.split(path)[0],os.getcwd())+os.sep)
    ep.preprocess(nb, {'metadata': {'path': path_dir}})

    if destination is None:
        destination=path
    with open(destination, 'wt') as f:
        nbformat.write(nb, f)

def get_notebook_outputs(path,inputs=None,progress=None,info=None):
    notebook = nbformat.read(path, as_version=4) #metadata: "report":{"show_markdown_progress":true,"include_markdown":false}
    show_markdown_progress=notebook.metadata['report']['show_markdown_progress'] if 'report' in notebook.metadata and 'show_markdown_progress' in notebook.metadata['report'] else False
    include_markdown=notebook.metadata['report']['include_markdown'] if 'report' in notebook.metadata and 'include_markdown' in notebook.metadata['report'] else False
    len_cells=len(notebook['cells'])

    if isinstance(inputs,dict):
        dict_to_environ(inputs)

    get_ipython().run_line_magic('matplotlib', 'widget')
    plt.ioff()

    scope=None
    res=[]
    for i,cell in enumerate(notebook['cells']):
        if cell['cell_type']=='code':
            code='\n'.join([line for line in cell['source'].split('\n') if not line.startswith('%')])
            capture,scope=capture_code(code,scope=scope)
            outputs=capture.outputs
            get_outputs_figure_widgets(outputs);
            res.extend(outputs)
        elif cell['cell_type']=='markdown' and ((info is not None and show_markdown_progress) or include_markdown):
            attachments = cell.get('attachments', {})
            renderer = IPythonRenderer(escape=False, attachments=attachments,anchor_link_text='',exclude_anchor_links=True)
            html=MarkdownWithMath(renderer=renderer).render(cell['source'])
            if info is not None and show_markdown_progress:
                info.children.append(html)
            if include_markdown:
                html=HTML(html)
                res.append(RichOutput(data={'text/plain':str(html),'text/html':html.data}))

        if progress is not None:
            progress.value = float(i+1)/len_cells
    plt.ion()
    return res
