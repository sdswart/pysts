from IPython.display import display, HTML,Image,Javascript,FileLink
from ipywidgets import widgets, interact, interact_manual, interactive, AppLayout, Button, Layout, GridspecLayout
import os, threading, urllib, math
from queue import Queue

from ..notebook import get_notebook_outputs, get_notebook_inputs
from .widget_utils import *
from ..convert import docx_to_pdf, create_word_from_html
from ..utils import get_output_path, outputs_to_html, outputs_to_output, get_outputs_figure_widgets
from .record import get_previous_report, get_previous_inputs, record_report

#Variables
notebook_dir=os.path.join(os.getcwd(),'Types')
report_dir=os.path.join(os.getcwd(),'Reports')
template_dir=os.path.join(os.getcwd(),'Templates')
data_dir='/home/jovyan/external/Server/BTR/Uploaded'

#Helpers
debug_view = widgets.Output(layout={'border': '1px solid black'})
wrap=create_wrap(debug_view)

#Status valid
valid='<span style="color: green;">●</span> Ready'
busy='<span style="color: orange;">●</span> Busy'
invalid='<span style="color: red;">●</span> Required'
isvalid={True:valid,False:invalid,'busy':busy}

def get_outputs_threaded(path,inputs,progress,info):
    que = Queue()
    kwargs=dict(path=path,inputs=inputs,progress=progress,info=info)
    #thread = threading.Thread(target=run_notebook_as_code, kwargs=kwargs)
    thread = threading.Thread(target=lambda q, arg1: q.put(get_notebook_outputs(**kwargs)), args=(que, kwargs))
    thread.start()
    thread.join()
    return que.get()

def get_outputs(path,inputs,progress,info):
    return get_notebook_outputs(path,inputs=inputs,progress=progress,info=info)

#Callbacks
def get_browser_options(path='/home/jovyan',allow=None,start_path='/home/jovyan'):
    root,dirs,files=next(os.walk(path))
    res=[] if start_path==path else ['...']
    res.extend([x for x in dirs if not x.startswith('.')])
    if allow is not None and not isinstance(allow,list):
        allow=[allow]
    res.extend([x for x in files if allow is None or any([x.lower().endswith(i.lower()) for i in allow])])
    return res

ALLOW_BROWSER=True
def change_path(browser):
    def rtn(*args,**kwargs):
        global ALLOW_BROWSER
        if browser.value is None or not ALLOW_BROWSER:
            return
        ALLOW_BROWSER=False
        if os.path.isfile(browser.path):
            browser.path=os.path.dirname(browser.path)
        fullpath=os.path.join(browser.path,browser.value)
        if browser.value=='...':
            fullpath=os.path.dirname(browser.path)
            browser.options=get_browser_options(fullpath,allow=browser.allow,start_path=browser.start_path)
            browser.path=fullpath
            browser.rows=len(browser.options)
            browser.value=None
        elif os.path.isdir(fullpath):
            browser.options=get_browser_options(fullpath,allow=browser.allow,start_path=browser.start_path)
            browser.path=fullpath
            browser.rows=len(browser.options)
            browser.value=None
        elif os.path.isfile(fullpath):
            browser.path=fullpath
        ALLOW_BROWSER=True
    return rtn

def upload_db(uploader,upload_to=None):
    if upload_to is None:
        upload_to=data_dir
    def rtn(*args,**kwargs):
        for uploaded_file in uploader.value:
            uploader.path=os.join(upload_to,uploaded_file["name"])
            with open(uploader.path, "wb") as fp:
                fp.write(uploaded_file["content"])
    return rtn

INPUTS={}
REQUIRED_INPUTS=[]
def get_inputs():
    global INPUTS
    return {key:str(widget.value if not hasattr(widget,'path') else widget.path if isinstance(widget.allow,list) and any([widget.path.endswith(x) for x in widget.allow]) else None) for key,widget in INPUTS.items()}

def inputs_ready():
    global REQUIRED_INPUTS
    inputs=get_inputs()
    return all([val not in ['','None','none',None] for key,val in inputs.items() if key in REQUIRED_INPUTS])



def create_input_widgets(report,inputs,callback=None):
    def rtn(*args,**kwargs):
        global INPUTS
        global REQUIRED_INPUTS
        INPUTS={}
        path=os.path.join(notebook_dir,report.value)+'.ipynb'
        children=[]
        REQUIRED_INPUTS=[]

        CUSTOM_KEYS=['required','type','label','upload_to','description']

        if os.path.isfile(path):
            ins=get_notebook_inputs(path)
            #h_children=[]
            for key,val in ins.items():
                var_type=val['type']

                kwargs={'layout':Layout(width='auto')}
                kwargs.update({key:val for key,val in val.items() if key not in CUSTOM_KEYS})

                if 'required' in val and val['required']==True:
                    REQUIRED_INPUTS.append(key)

                if var_type=='autofill':
                    options=get_previous_inputs(notebook=path,inputs=[key])
                    kwargs.update(dict(options=options,ensure_option=False,disabled=False))
                    widget=widgets.Combobox(**kwargs)
                elif var_type=='upload':
                    widget=widgets.FileUpload(**kwargs)
                    widget.path=None
                    upload_to=val['upload_to'] if 'upload_to' in val else None
                    wrap(widget)(upload_db(widget,upload_to))
                elif var_type=='path':
                    widget=widgets.Select(**kwargs)
                    widget.path=val['path'] if 'path' in val else '/home/jovyan'
                    widget.start_path=val['start_path'] if 'start_path' in val else '/home/jovyan'
                    widget.allow=val['allow'] if 'allow' in val else None
                    widget.options=get_browser_options(path=widget.path,allow=widget.allow,start_path=widget.start_path)
                    widget.value=None
                    wrap(widget)(change_path(widget))
                elif hasattr(widgets,var_type):
                    widget=getattr(widgets,var_type)(**kwargs)
                else:
                    continue

                #new_children=[]
                label=val['label'] if 'label' in val else key
                children.append(widgets.HTML(label+':'))
                if 'description' in val:
                    children.append(widgets.HTML(f'<p style="color:#545b62;">{val["description"]}</p>'))
                children.append(widget)
                #children.append(widgets.VBox(children=new_children,layout=Layout(width='100%')))

                #if len(h_children)==2:
                #    children.append(widgets.HBox(h_children,layout=Layout(width='auto')))
                #    h_children=[]
                if callback is not None:
                    wrap(widget)(callback(widget))

                INPUTS[key]=widget

        #children.extend(h_children)
        inputs.children=children
    return rtn

def show_results(paths,info,download):
    info.value='Complete! You can download your report here:'
    res=[f'<p><a href="/user-redirect/files/{urllib.parse.quote(path.replace("/home/jovyan",""))}" target="_blank">{os.path.split(path)[-1]}</a></p>' for path in paths]
    download.value=(''.join(res))

OUTPUTS=[]

def run_code_fcn(report,exec_code_info,progress,info,notebook_output,edit_notebook,edit_figs,generate_report):
    @debug_view.capture(clear_output=True)
    def rtn(*_,notebook_path=None,inputs=None):
        global OUTPUTS
        if notebook_path is None:
            notebook_path=os.path.join(notebook_dir,f'{report.value}.ipynb')
        if inputs is None:
            inputs=get_inputs()
        #Execute code
        exec_code_info.children=[widgets.HTML(isvalid['busy']),progress]
        progress.value=.0
        progress.layout.display = "flex"
        print(notebook_path, inputs)
        OUTPUTS=get_outputs(path=notebook_path,inputs=inputs,progress=progress,info=info)
        exec_code_info.children=[widgets.HTML(isvalid[True])]
        edit_notebook.disabled = False
        edit_figs.disabled = False
        generate_report.disabled = False
        notebook_output.clear_output()
        progress.layout.display = "none"
    return rtn

def generate_report_fcn(report,docx_gen_info,pdf_gen_info,template,generate_pdf,info,download):
    def rtn(*_,notebook_path=None,inputs=None,display_results=True):
        global OUTPUTS
        if notebook_path is None:
            notebook_path=os.path.join(notebook_dir,f'{report.value}.ipynb')
        if inputs is None:
            inputs=get_inputs()
        #Generate report
        docx_gen_info.children=[widgets.HTML(isvalid['busy'])]
        info.value='Generating report...'
        download.value=''
        output_path=get_output_path(notebook_path,inputs=inputs,folder=report_dir,extension='docx')
        template_path=os.path.join(template_dir,f'{template.value}.docx')
        html=outputs_to_html(OUTPUTS,types=['text/html','application/vnd.jupyter.widget-view+json'])
        paths=create_word_from_html(html,template_path,output_path=output_path)
        docx_gen_info.children=[widgets.HTML(isvalid[True])]

        #Generate pdf
        if generate_pdf.value:
            info.value='Generating PDF...'
            pdf_gen_info.children=[widgets.HTML(isvalid['busy'])]
            paths.extend(docx_to_pdf(paths))
            pdf_gen_info.children=[widgets.HTML(isvalid[True])]

        record_report(notebook_path=notebook_path,inputs=inputs,paths=paths)
        if display_results:
            show_results(paths,info,download)
        return paths
    return rtn

def reset_exec_report_status(exec_code_info,docx_gen_info,pdf_gen_info,edit_notebook,edit_figs,generate_report,notebook_output):
    def rtn(*_):
        exec_code_info.children=[widgets.HTML(isvalid[False])]
        docx_gen_info.children=[widgets.HTML(isvalid[False])]
        pdf_gen_info.children=[widgets.HTML(isvalid[False])]
        edit_notebook.disabled = True
        edit_figs.disabled = True
        generate_report.disabled = True
        notebook_output.clear_output()
    return rtn

def reset_and_run_code(report,progress,info,exec_code_info,docx_gen_info,pdf_gen_info,edit_notebook,edit_figs,generate_report,notebook_output):
    def rtn(*_):
        reset_exec_report_status(exec_code_info,docx_gen_info,pdf_gen_info,edit_notebook,edit_figs,generate_report,notebook_output)()
        run_code_fcn(report,exec_code_info,progress,info,notebook_output,edit_notebook,edit_figs,generate_report)()
    return rtn

def run_all(report,template,progress,info,download,force_execution,generate_pdf,exec_code_info,docx_gen_info,pdf_gen_info,
            edit_notebook,edit_figs,generate_report,notebook_output):
    def rtn(*_):
        #reset status
        reset_exec_report_status(exec_code_info,docx_gen_info,pdf_gen_info,edit_notebook,edit_figs,generate_report,notebook_output)()
        notebook_path=os.path.join(notebook_dir,f'{report.value}.ipynb')
        inputs=get_inputs()

        #Check for previous Reports
        paths=get_previous_report(notebook_path,inputs)
        if force_execution.value or paths is None:
            #Execute code
            run_code_fcn(report,exec_code_info,progress,info,notebook_output,edit_notebook,edit_figs,generate_report)(notebook_path=notebook_path,inputs=inputs)

            #Generate report
            paths=generate_report_fcn(report,docx_gen_info,pdf_gen_info,template,generate_pdf,info,download)(notebook_path=notebook_path,inputs=inputs,display_results=False)

        show_results(paths,info,download)
    return rtn

#Status checks
def set_generate_button_disabled(generate,options_info,inputs_info,run_code):
    def rtn():
        generate.disabled = run_code.disabled = not ((options_info.valid if hasattr(options_info,'valid') else False) and (inputs_info.valid if hasattr(inputs_info,'valid') else False))
    return rtn

def check_options(report,options_info,complete_fcn=None):
    def rtn(*_):
        valid=report.value !='' and report.value is not None
        options_info.children=[widgets.HTML(isvalid[valid])]
        options_info.valid=valid
        if complete_fcn is not None:
            complete_fcn()
    return rtn

def check_inputs(inputs_info,complete_fcn=None):
    def rtn(widget):
        def sub_rtn(*_):
            valid=inputs_ready()
            inputs_info.children=[widgets.HTML(isvalid[valid])]
            inputs_info.valid=valid
            if complete_fcn is not None:
                complete_fcn()
        return sub_rtn
    return rtn

def show_pdf_info(generate_pdf,pdf_info_box):
    def rtn(*_):
        pdf_info_box.layout.display = 'flex' if generate_pdf.value else 'none'
    return rtn

def add_report_to_output(output):
    def rtn(*_):
        global OUTPUTS
        output.clear_output()
        output.append_display_data(HTML('Collecting report outputs...'))
        output.clear_output()
        for out in OUTPUTS:
            if 'text/html' in out.data:
                out.data['text/html']='<div contenteditable="false">'+out.data['text/html']+'</div>'
        outputs_to_output(OUTPUTS,output)
    return rtn

def create_limit_widget(ax,axis_type):
    lim=ax.get_xlim() if axis_type=='x' else ax.get_ylim()
    diff=abs(lim[1]-lim[0])
    readout_format=f'.{int(max([0,math.log10(10/diff)]))}f'
    desc='X-axis:' if axis_type=='x' else 'Y-axis:'
    widget=widgets.FloatRangeSlider(value=lim,min=lim[0]-(diff*0.1),max=lim[1]+(diff*0.1),step=diff/100,description=desc,disabled=False,continuous_update=False,orientation='horizontal',readout=True,readout_format=readout_format)

    def rtn(*_):
        if axis_type=='y':
            ax.set_ylim(widget.value)
        elif axis_type=='x':
            ax.set_xlim(widget.value)

    wrap(widget)(rtn)
    return widget

def create_legend_widget(ax):
    if any([x.get_label() not in [None, '', ' '] for x in ax.lines]) and len(ax.lines)>1:
        options=['best','upper right','upper left','lower left','lower right','right','center left','center right','lower center','upper center','center']
        widget=widgets.Dropdown(value='best',options=options,rows=5,layout=Layout(width='120px'))

        def rtn(*_):
            ax.legend(loc=widget.value)

        wrap(widget)(rtn)
        return [widgets.HTML('Legend:'),widget]
    return []

def add_figs_to_output(output):
    def rtn(*_):
        global OUTPUTS
        output.clear_output()
        output.append_display_data(HTML('Collecting figures...'))
        canvases=get_outputs_figure_widgets(OUTPUTS)

        res=[]
        complete_ax=[]
        def get_title(ax):
            title=ax.get_title()
            return f'{title+" - " if title not in [None,""," "] else ""}{ax.get_ylabel()}'
        for canvas in canvases:
            children=[]
            len_axes=len(canvas.figure.axes)
            for i,ax in enumerate(canvas.figure.axes):
                if ax in complete_ax:
                    continue
                shared=list(ax.get_shared_x_axes())
                shared_x_axes=[shared_ax for x in shared if ax in x for shared_ax in x]
                shared=list(ax.get_shared_y_axes())
                shared_y_axes=[shared_ax for x in shared if ax in x for shared_ax in x]

                if len(shared_x_axes)>0:
                    res.append(widgets.HBox([widgets.HTML(f' ',layout=Layout(width='200px')),create_limit_widget(ax,axis_type='x')],layout=Layout(width='auto')))
                    for shared_ax in shared_x_axes:
                        res.append(widgets.HBox([widgets.HTML(f'<strong>{get_title(shared_ax)}:</strong>',layout=Layout(width='200px')),create_limit_widget(shared_ax,axis_type='y'),*create_legend_widget(shared_ax)],layout=Layout(width='auto')))
                        complete_ax.append(shared_ax)
                elif len(shared_y_axes)>0:
                    res.append(widgets.HBox([widgets.HTML(f' ',layout=Layout(width='200px')),create_limit_widget(ax,axis_type='y')],layout=Layout(width='auto')))
                    for shared_ax in shared_y_axes:
                        res.append(widgets.HBox([widgets.HTML(f'<strong>{get_title(shared_ax)}:</strong>',layout=Layout(width='200px')),create_limit_widget(shared_ax,axis_type='x'),*create_legend_widget(shared_ax)],layout=Layout(width='auto')))
                        complete_ax.append(shared_ax)
                else:
                    res.append(widgets.HBox([widgets.HTML(f'<strong>{get_title(ax)}:</strong>',layout=Layout(width='200px')),create_limit_widget(ax,axis_type='x'),create_limit_widget(ax,axis_type='y'),*create_legend_widget(ax)],layout=Layout(width='auto')))
                    complete_ax.append(ax)

            res.append(widgets.Box([canvas],layout=Layout(width='100%',border="1px solid grey")))
            #canvas.manager.toolbar._Button("PREVIOUS", "back_large", self.prev)
        output.clear_output()
        outputs_to_output(res,output)
    return rtn


# CREATE APP
def create_app():
    inputs=widgets.VBox(layout=Layout(width='100%'))
    notebook_options=[x[:-6] for x in os.listdir(notebook_dir) if x.endswith('.ipynb')]
    val='Snapshot' if 'Snapshot' in notebook_options else notebook_options[0]
    report=widgets.Dropdown(options=notebook_options,rows=5,value=val,layout=Layout(width='auto'))
    template_options=['None']+[x[:-5] for x in os.listdir(template_dir) if x.endswith('.docx')]
    template_val='template' if 'template' in template_options else template_options[0]
    template=widgets.Dropdown(options=template_options,rows=5,value=template_val,layout=Layout(margin='0px 30px 0px 0px'))
    generate_pdf=widgets.Checkbox(value=False,description='Create PDF',disabled=False,indent=False)
    force_execution=widgets.Checkbox(value=True,description='Force Execution',disabled=False,indent=False)

    options=widgets.HBox([widgets.HTML('Template:'),template,generate_pdf,force_execution],layout=Layout(width='100%',padding='10px 0px 0px 0px'))

    #Status
    progress = widgets.FloatProgress(value=0.0, min=0.0, max=1.0)
    progress.layout.display = "none"

    options_info=widgets.HBox([widgets.HTML(invalid)],layout=Layout(width='auto'))
    inputs_info=widgets.HBox([widgets.HTML(invalid)],layout=Layout(width='auto'))
    exec_code_info=widgets.HBox([widgets.HTML(invalid)],layout=Layout(width='auto'))
    docx_gen_info=widgets.HBox([widgets.HTML(invalid)],layout=Layout(width='auto'))
    pdf_gen_info=widgets.HBox([widgets.HTML(invalid)],layout=Layout(width='auto'))

    pdf_info_box=widgets.HBox([widgets.HTML('<strong>PDF Generation:</strong>'),pdf_gen_info],layout=Layout(width='auto'))
    pdf_info_box.layout.display = "none"
    status=widgets.VBox([
        widgets.HBox([widgets.HTML('<strong>Report Options:</strong>'),options_info],layout=Layout(width='auto')),
        widgets.HBox([widgets.HTML('<strong>Report Inputs:</strong>'),inputs_info],layout=Layout(width='auto')),
        widgets.HBox([widgets.HTML('<strong>Code Execution:</strong>'),exec_code_info],layout=Layout(width='auto')),
        widgets.HBox([widgets.HTML('<strong>Document Generation:</strong>'),docx_gen_info],layout=Layout(width='auto')),
        pdf_info_box
    ],layout=Layout(width='500px'))


    info=widgets.HTML()
    download=widgets.HTML()
    generate=widgets.Button(description='Run Code and Generate Report',disabled=True,button_style='primary', icon='cogs',layout=Layout(width='auto'))
    results=widgets.VBox([generate,info,download],layout=Layout(width='auto'))

    top_area=widgets.HBox([status,results],layout=Layout(width='auto'))

    #Edit
    run_code=widgets.Button(description='Run Code',disabled=True,button_style='primary', icon='play')
    generate_report=widgets.Button(description='Generate Report',disabled=True,button_style='primary', icon='cogs')
    edit_notebook=widgets.Button(description='Edit Report',disabled=True,button_style='primary', icon='file-text-o')
    edit_figs=widgets.Button(description='Edit Figures',disabled=True,button_style='primary', icon='line-chart')
    notebook_output=widgets.Output(layout={'border': '1px solid black'})
    notebook_output.append_display_data(HTML('Please run code first'))

    #Callbacks
    #Change inputs
    generate_disabled=set_generate_button_disabled(generate,options_info,inputs_info,run_code)
    callback=check_inputs(inputs_info,complete_fcn=generate_disabled)
    fcn=create_input_widgets(report,inputs,callback=callback)
    wrap(report)(fcn)
    fcn()


    fcn=check_options(report,options_info,complete_fcn=generate_disabled)
    wrap(report)(fcn)
    fcn()

    fcn=show_pdf_info(generate_pdf,pdf_info_box)
    wrap(generate_pdf)(fcn)
    fcn()

    run_code.on_click(reset_and_run_code(report,progress,info,exec_code_info,docx_gen_info,pdf_gen_info,edit_notebook,edit_figs,generate_report,notebook_output))
    edit_notebook.on_click(add_report_to_output(notebook_output))
    edit_figs.on_click(add_figs_to_output(notebook_output))
    generate_report.on_click(generate_report_fcn(report,docx_gen_info,pdf_gen_info,template,generate_pdf,info,download))

    generate.on_click(run_all(report,template,progress,info,download,force_execution,generate_pdf,exec_code_info,docx_gen_info,pdf_gen_info,
                edit_notebook,edit_figs,generate_report,notebook_output))

    next_button=widgets.Button(description='Next',disabled=False,button_style='primary', icon='arrow-right')
    content = widgets.Tab(children=[
                widgets.VBox([report,options,next_button],layout=Layout(width='auto')),
                widgets.VBox([inputs,next_button],layout=Layout(width='auto')),
                widgets.VBox([widgets.HBox([run_code,edit_notebook,edit_figs,generate_report],layout=Layout(width='auto')),notebook_output],layout=Layout(width='auto'))
            ], layout=Layout(width='auto',border='1px solid grey'))

    titles={0:'Options',1:'Inputs',2:'Edit'}
    for i,val in titles.items():
        content.set_title(i, val)
    content.selected_index=0

    def go_next(*_):
        content.selected_index+=1
    next_button.on_click(go_next)

    app=widgets.VBox([widgets.HTML('<h1>Generate Report</h1>'),top_area,content],
            layout=Layout(width='100%'))
    return app
