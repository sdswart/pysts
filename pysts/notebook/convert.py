import os
from .utils import *

def libre_convert_to_pdf(paths):
    LIBRE_OFFICE=''
    res=[]
    for path in paths:
        if path.endswith('docx'):
            pdf_path=path.replace('.docx','.pdf')
            p = Popen([LIBRE_OFFICE, '--headless', '--convert-to', 'pdf', '--outdir',
                       pdf_path, path])
            p.communicate()
            res.append(pdf_path)
    return res

def docx_to_pdf(paths):
    #docx2pdf=get_package('docx2pdf')
    pypandoc=get_package('pypandoc')
    res=[]
    for path in paths:
        if path.endswith('docx'):
            pdf_path=path.replace('.docx','.pdf')
            #docx2pdf.convert(path, pdf_path)
            pypandoc.convert_file(path, 'pdf', outputfile=pdf_path,extra_args=['--pdf-engine=xelatex'])
            res.append(pdf_path)
    return res

def create_word_from_html(html,template_path,output_path=None):
    pypandoc=get_package('pypandoc')
    docx=get_package('docx','python-docx')
    Document=docx.Document

    with open("temp.html",'w') as f:
        f.write(html)
    pypandoc.convert_file('temp.html', 'docx', outputfile="temp.docx")
    temp_doc=Document("temp.docx")

    if template_path is not None and os.path.isfile(template_path):
        document = Document(template_path)

        #Add elements
        for elem in temp_doc.element.body:
            document.element.body.append(elem)

        #Add image parts
        for imagepart in temp_doc.part._package.image_parts:
            document.part._package.image_parts.append(imagepart)
        for rId,rel in temp_doc.part.rels.items():
             if rel.reltype.endswith('image'):
                document.part.rels[rId]=rel
                document.part.rels._target_parts_by_rId[rId] = rel._target
    else:
        document=temp_doc

    #change table styles
    for table in document.tables:
        table.style = 'default'
        for row in table.rows:
            for cell in row.cells:
                for paragraph in cell.paragraphs:
                    paragraph.style='table_normal'

    if output_path is None:
        report_name=f'Report_{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}.docx'
        output_path=os.path.join(os.getcwd(),report_name)

    document.save(output_path)
    os.remove('temp.docx')
    os.remove('temp.html')

    return [output_path]
