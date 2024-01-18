import pyodbc
import httplib2
from urllib import parse
import datetime
import traceback
import sys
import pandas as pnd
import json
from collections import OrderedDict
from xml.sax.saxutils import escape
import xml.etree.ElementTree as ET
import re
import numpy as np
import chardet
from PyQt5.QtWidgets import QApplication, QWidget, QPushButton, QVBoxLayout ,  QFileDialog, QButtonGroup, QRadioButton, QLineEdit, QLabel
from PyQt5.QtCore import Qt

app=None
window=None
lab_file_mapping=None
lab_file_source=None
input_solr_endpoint=None

SRC_FILE=""
MAPPING_FILE=""
SOLR_ENDPOINT="https://geocase.cetaf.org/geocase_solr/solr/geocase_init/"
SOLR_AUTH=False
SOLR_USER=""
SOLR_PWD=""

CHECK_URL="select?indent=true&q.op=OR&q="
CHECK_FIELD="geocase_id"

field_mapping={}


def print_time():
    now = datetime.datetime.now()
    print ("Current date and time : ")
    print (now.strftime("%Y-%m-%d %H:%M:%S"))
    
    
def check_already_exists(p_solr_endpoint, p_check_query, p_fieldname, p_key):
    #print(p_key)
    encoded_key=parse.quote_plus(p_key)
    #print(encoded_key)
    check_url=p_solr_endpoint+p_check_query+p_fieldname+"%3A"+encoded_key
    #print(check_url)
    p_h = httplib2.Http(".cache")
    resp, content = p_h.request(check_url, "GET" )
    #print(resp)
    #print(content)
    content_dict=json.loads(content)
    returned= None
    if "response" in content_dict:
        if "docs" in content_dict["response"]:
            if len(content_dict["response"]["docs"])>0:
                tmp_doc=content_dict["response"]["docs"][0]
                if "id" in tmp_doc:
                    returned=tmp_doc["id"]
    #print("======================>")
    #print(returned)
    return returned

def add_solr(p_solr_url,  p_fields, list_multi_fields=None , p_auth=False, p_solr_user=None, p_solr_password=None ):
    p_h = httplib2.Http(".cache")
    if p_auth:
        p_h.add_credentials(p_solr_user, p_solr_password)
    insert_url=p_solr_url+"update"
    commit_url=p_solr_url+"update?commit=true"
    list_fields=[]
    for k, v in p_fields.items():
        if len(str(v) or "") >0:
            list_fields.append("<field name='"+k+"'>"+escape(str(v))+"</field>")
            
    
    if not list_multi_fields is  None:
        for k, tmp in list_multi_fields.items():
            for v in tmp:
                list_fields.append("<field name='"+k+"'>"+escape(str(v))+"</field>")
    
    xml="<add><doc>"+"".join(list_fields)+"</doc></add>"
    
    resp, content = p_h.request(insert_url, "GET", body=xml.encode('utf-8'), headers={'content-type':'application/xml', 'charset':'utf-8'} )
    check_xml = ET.fromstring(content)
    stat=check_xml.findall(".//int[@name='status']")
    if(len(stat)>0):
        if str(stat[0].text).strip()!="0":
            print("Error - return code ="+ str(stat[0].text).strip() )
            print(p_fields)
            print(xml)
            print(content)
    else:
        print("Error no return code")
        print(p_fields)
        print(xml)
    resp2, content2= p_h.request(commit_url)
    
    

def get_encoding(p_file):
    detector = chardet.universaldetector.UniversalDetector()
    with open(p_file, "rb") as f:
        for line in f:
            detector.feed(line)
            if detector.done:
                break
    detector.close()
    #print(detector.result)
    return detector.result["encoding"] or ""

def create_mapping(df_mapping):
    global field_mapping
    df_mapping=df_mapping.replace(np.nan, "")
    for index, row in df_mapping.iterrows():
        solr_field=row["solr_field"]
        source_field=row["source_field"]
        if len(solr_field)>0  and len(source_field)>0 :
            if not source_field in field_mapping:
                field_mapping[source_field]=[]
            if not solr_field in field_mapping[source_field]:
                field_mapping[source_field].append(solr_field)    
            



def parse_csv(p_src, p_mapping, p_solr_endpoint, p_auth=False, p_solr_user=None, p_solr_password=None):
    global field_mapping
    global CHECK_FIELD
    global CHECK_URL
    encoding_mapping=get_encoding(p_mapping)
    df_mapping=pnd.read_csv(p_mapping, sep="\t", header=0, encoding=encoding_mapping)
    create_mapping(df_mapping)
    
    encoding_src=get_encoding(p_src)
    df_src=pnd.read_csv(p_src, sep="\t", header=0,encoding=encoding_src)
    df_src=df_src.replace(np.nan, "")
    
    print("Got main data")
    print_time()
    for index, row in df_src.iterrows():
        #print(row)
        doc={}
        for src_field, key_array in field_mapping.items():            
            if len((str(row[src_field]) or "").strip())>0:
                val=str(row[src_field]).strip()
                
                for solr_field in key_array:
                    doc[solr_field]=val
        if CHECK_FIELD in doc:
            exist_id=check_already_exists(p_solr_endpoint, CHECK_URL, CHECK_FIELD, doc[CHECK_FIELD]) or ""
            if len(exist_id)>0:
                doc["id"]=exist_id
            add_solr(p_solr_endpoint, doc, list_multi_fields=None , p_auth=False, p_solr_user=None, p_solr_password=None )
        if index%100==0:
            print(index)
    print("done")
    print_time()
        
def load_data():
    global MAPPING_FILE
    global SRC_FILE
    global input_solr_endpoint
    solr_endpoint=input_solr_endpoint.text() or ""
    if len(MAPPING_FILE) >0 and len(SRC_FILE)>0 and len(solr_endpoint)>0:
        parse_csv(SRC_FILE, MAPPING_FILE, solr_endpoint)
    
def choose_mapping():
    global MAPPING_FILE
    global lab_file_mapping
    global window
    file_name = QFileDialog()
    options = QFileDialog.Options()
    options |= QFileDialog.DontUseNativeDialog
    filter = "txt (*.txt);;csv (*.csv)"
    file_name.setFileMode(QFileDialog.ExistingFiles)
    MAPPING_FILE=file_name.getOpenFileName(window, "Open files", "", filter, options=options)[0] or ""
    print(MAPPING_FILE)
    lab_file_mapping.setText("Mapping file : " +MAPPING_FILE )

def choose_solr():
    global SRC_FILE
    global lab_file_source
    global window
    file_name = QFileDialog()
    options = QFileDialog.Options()
    options |= QFileDialog.DontUseNativeDialog
    filter = "txt (*.txt);;csv (*.csv)"
    file_name.setFileMode(QFileDialog.ExistingFiles)
    SRC_FILE=file_name.getOpenFileName(window, "Open files", "", filter, options=options)[0] or ""
    print(SRC_FILE)
    lab_file_source.setText("Source file (data) : " +SRC_FILE )
   
def start():
    global app
    global window
    global lab_file_mapping
    global lab_file_source
    global input_solr_endpoint
    global SOLR_ENDPOINT
    app = QApplication([])
    window = QWidget()
    window.setMinimumWidth(500)
    layout = QVBoxLayout()
    
    lab_file_mapping=QLabel()
    lab_file_mapping.setText("Mapping file :")
    layout.addWidget(lab_file_mapping)
    
    but_file_mapping=QPushButton('Choose file for SOLR mapping')
    layout.addWidget(but_file_mapping)
    but_file_mapping.clicked.connect(choose_mapping)
    
    lab_file_source=QLabel()
    lab_file_source.setText("Source file (data) :")
    layout.addWidget(lab_file_source)
    
    but_file_src=QPushButton('Choose source CSV')
    layout.addWidget(but_file_src)
    but_file_src.clicked.connect(choose_solr)
    
    lab_solr_endpoint=QLabel()
    lab_solr_endpoint.setText("SOLR end point :")
    layout.addWidget(lab_solr_endpoint)
    
    input_solr_endpoint=QLineEdit()
    input_solr_endpoint.setText(SOLR_ENDPOINT)
    layout.addWidget(input_solr_endpoint)
    
    but_proceed=QPushButton('Load data')
    layout.addWidget(but_proceed)
    but_proceed.clicked.connect(load_data)
    
    
    
    
    window.setLayout(layout)
    window.setWindowFlags(Qt.WindowStaysOnTopHint)
    window.show()
    app.exec()
    
    

if __name__ == '__main__':
    start()




