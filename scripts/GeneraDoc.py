
import pandas as pd  # también instalar pip install openpyxl
import re
from docxtpl import DocxTemplate
from datetime import datetime
from googletrans import Translator
import win32com.client as win32
import os
from collections import Counter


PATH_TEMPLATES = '../Templates/'
PATH_PLANTILLAS = '{}Plantillas/'.format(PATH_TEMPLATES)
PATH_OUTPUT = '../Output/'
DOC_MC = 'MC_BI.docx'
DOC_MO = 'MO_BI.docx'
DOC_SWP = 'SWP_BI.docx'
EXCEL_SWP = 'SWP_BI.xlsx'
DOC_COPR = 'COPR.docx'

ICON_EXCEL = os.path.abspath('../icoExcel.ico')


def changeKindTemplate(tipoTemplate):
    tiposTemplate={'SPTdt':'Ficha Stored Procedure Teradata',
                   'SPNz':'Ficha Stored Procedure Netezza',
                   'ExTdt':'Ficha Extracto de data desde Teradata',
                   'ExNz': 'Ficha Extracto de data desde Netezza',
                   'NzLoad':'Ficha de Carga de data a Netezza',
                   'RNNz':'Ficha de Carga de data a Netezza',
                   'Schd':'Ficha de automatización'}
    proceso= tiposTemplate[tipoTemplate]
    return proceso

def readExcel(fileName):
    fileSchedule = os.path.abspath("{}{}".format(PATH_TEMPLATES, fileName))
    pdSchedule = pd.read_excel(fileSchedule, sheet_name='Schedule', skiprows=1)
    pdSchedule = pdSchedule.rename(columns={
                                   'Nombre Layout': 'layout', 'Tipo Schedule': 'tipoTemplate', 'Archivo Template': 'nameTemplate'})
    pdSelSchedule = pdSchedule.iloc[:, [1, 2, 14]].dropna()
    return pdSelSchedule


def correCaracter(filename):
    invalid_chars_pattern = r'[<>:"/\\|?*]'
    cadCorregida = re.sub(invalid_chars_pattern, '_', filename)
    return cadCorregida


def adjuntarTemplate():
    archivo_adjunto = "SPTdt_PLTCHRNFJ.xlsx"


def main(pdProceso):

    schemaFiles = pdProceso.apply(lambda row: row.to_dict(), axis=1).tolist()

    
    layout_nz=[layout for layout in schemaFiles if layout['tipoTemplate'] in ('RNNz','NzLoad')]
    layout_count_nz=Counter(layout['layout'] for layout in layout_nz)

    layout_duplicados={layout for layout,count in layout_count_nz.items() if count>1}
    print(layout_duplicados)
    
    nzDelete='RNNz'
    print(schemaFiles)
    schemaFiles=[template for template in schemaFiles if not (template['layout'] in layout_duplicados and template['tipoTemplate']==nzDelete)]
    print(schemaFiles)
    
    
 #   print(layout_duplicados)
    
    #cNzload = False
    #cRNNz = False
    
    #lambda diccionarios: any(filter(lambda schema: schema['tipoTemplate'] == 'NzLoad', schemaFiles) 
    #                        and filter(lambda schema: schema['tipoTemplate'] == 'RNNz', schemaFiles)
    
    """
    tiene_duplicados_con_layout = lambda diccionarios: (
                    (any(map(lambda schema: schema['tipoTemplate'] == 'RNNz', schemaFiles)) and
                    any(map(lambda schema: schema['tipoTemplate'] == 'NzLoad', schemaFiles))) and
                    any(map(lambda layout: len(list(filter(lambda dic: dic['layout'] == layout, diccionarios))) > 1,
                    set(dic['layout'] for dic in diccionarios)))
    )
    """
    
  
    for diccionario in schemaFiles:
        updateNameFile = "{}{}_ExcelP".format(
            PATH_TEMPLATES, diccionario['nameTemplate'])
        diccionario['fileAdjunto'] = updateNameFile
        diccionario['tipoTemplate']= "{}:\n{}".format(changeKindTemplate(diccionario['tipoTemplate']),diccionario['layout'])

    docPlantilla = os.path.abspath("{}{}".format(PATH_PLANTILLAS, DOC_MC))
    feat = 123
    regu = 231
    description = correCaracter('Canastas D –Formato 2B LA/LC')
    fechaUpdate = datetime.today().strftime("%Y-%m-%d")
    month = datetime.today().strftime('%B')
    monthCreated = 'Junio' #Translator().translate(month, src='en', dest='es').text
    yearCreated = datetime.today().strftime('%Y')

    doc = DocxTemplate(docPlantilla)

    valores = {
        'numberFeat': feat,
        'numberRegu': regu,
        'description': description,
        'dateDoc': fechaUpdate,
        'monthCreated': monthCreated,
        'yearCreated': yearCreated,
        'registros': schemaFiles
    }

    #print(valores)

    finalFile = os.path.abspath(
        '{}FEAT-{}_REGU-{}_MC-BI {}.docx'.format(PATH_OUTPUT, feat, regu, description))

    doc.render(valores)
    doc.save(finalFile)

    
    word_app = win32.gencache.EnsureDispatch("Word.Application")
    word_app.Visible = False
    doc_word = word_app.Documents.Open(finalFile)

    foundMarFile= False
    try:
        for paragraph in doc_word.Paragraphs:
            for diccionario in schemaFiles:
                valorFind= diccionario['fileAdjunto']
                
                if valorFind in paragraph.Range.Text:
                    print('Encontrado:{}'.format(diccionario['fileAdjunto'].replace('_ExcelP','')))
                    fileAdjunto=os.path.abspath(diccionario['fileAdjunto'].replace('_ExcelP',''))
                    paragraph.Range.Text = paragraph.Range.Text.replace(valorFind, '')
                    ole_shape = paragraph.Range.InlineShapes.AddOLEObject(
                        ClassType="Excel.Sheet.12",
                        FileName=fileAdjunto,
                        LinkToFile=False,
                        DisplayAsIcon=True,
                        IconFileName=ICON_EXCEL,
                        IconIndex=0,    
                        IconLabel=diccionario['nameTemplate']
                    )
                    break
            
    except Exception as e:
        print(f"Error al acceder a los párrafos del documento: {e}")
        doc_word.Close()
        word_app.Quit()
        #os.remove(temp_doc_path)
        raise
    

    print('pin36')
    doc_word.Close()
    word_app.Quit()


if __name__ == "__main__":
    print('prueba')
    tabla = readExcel('Sched_PLT_CHRN_FJ.xlsx')
    main(tabla)
