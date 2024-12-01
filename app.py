import pickle
import locale
from collections import namedtuple
from tarfile import NUL
from turtle import width
from app2 import main2
import threading
import streamlit as st
import requests
import pandas as pd
import json
import streamlit_highcharts as hct
import asyncio

import streamlit as st
from azure.storage.blob import BlobServiceClient
import requests
import json
import uuid
import time
from PIL import Image
from io import BytesIO
import ast
# Configuration
FUNCTION_BASE_URL = "http://localhost:7190/api" # e.g., https://<function-app>.azurewebsites.net/api/
version="0.1a"
#FUNCTION_BASE_URL = "https://alexfuncdoc.azurewebsites.net/api" # e.g., https://<function-app>.azurewebsites.net/api/

GENERATE_SAS_TOKEN_ENDPOINT = f"{FUNCTION_BASE_URL}/GenerateSASToken"
START_ORCHESTRATOR_ENDPOINT = f"{FUNCTION_BASE_URL}/start-orchestrator"
EXTRACT_COLUMNS_ENDPOINT = f"{FUNCTION_BASE_URL}/start-column-extraction"  # New endpoint for column extraction
CHECK_JOB_STATUS_ENDPOINT = f"{FUNCTION_BASE_URL}/check-job-status"
API_URL_DATA = f"{FUNCTION_BASE_URL}/GetPaginatedData"
storage_connection_string="DefaultEndpointsProtocol=https;AccountName=vsstoragelake;AccountKey=uxCGrVpPSWf5lJRgCc8YAzkyoXONMvOcYtC2N0cfcCbriOYwNNBHA6wMU+oiUmcN4Hgc0gr3ZCO7+AStzZMAlw==;EndpointSuffix=core.windows.net"
storage_account = "vsstoragelake"
container = "testcontainer"

class Task:
    def __init__(self, id):
        self.id = id
        self.progress = 0
        self.status = 'Running'
        self.weight = 25
        self.count=0
        self.name=""
        self.amount=0
        self.lock = threading.Lock()

    def update_progress(self, value):
        with self.lock:
            self.progress = value

    def set_status(self, status):
        with self.lock:
            self.status = status

    def get_progress(self):
        with self.lock:
            return self.progress

    def get_status(self):
        with self.lock:
            return self.status

    def get_weight(self):
        with self.lock:
            return self.weight
    def set_weight(self, weight):
        with self.lock:
            self.weight= weight
    def get_name(self):
        with self.lock:
            return self.name
    def set_name(self, name):
        with self.lock:
            self.weight= name

#FUNCTION_KEY = os.getenv("FUNCTION_KEY")  # If using function keys for authentication
FUNCTION_KEY=""
test_data = {
        "UnusualTest": {
            "status": "Not started",
            "start_time": None,
            "end_time": None,
            "error": None,
            "name": "Unusual Account Combinations",
            "weight": 50,
            "parameters": {},
            "count": 0,
            "sumAmount": 0
        },
        "DuplicateTest": {
            "status": "Not started",
            "start_time": None,
            "end_time": None,
            "error": None,
            "name": "Duplicate Entries",
            "weight": 50,
            "parameters": {},
            "count": 0,
            "sumAmount": 0
        },
        "OutlierTest": {
            "status": "Not started",
            "start_time": None,
            "end_time": None,
            "error": None,
            "name": "Outlier Amounts Detection",
            "weight": 50,
            "parameters": {},
            "count": 0,
            "sumAmount": 0
        },
        "RoundedTest": {
            "status": "Not started",
            "start_time": None,
            "end_time": None,
            "error": None,
            "name": "Rounded Amounts",
            "weight": 50,
            "parameters": {},
            "count": 0,
            "sumAmount": 0
        },
        "WeekendTest": {
            "status": "Not started",
            "start_time": None,
            "end_time": None,
            "error": None,
            "name": "Entries Posted During Weekends",
            "weight": 50,
            "parameters": {},
            "count": 0,
            "sumAmount": 0
        },
            "SuspiciousTest": {
            "status": "Not started",
            "start_time": None,
            "end_time": None,
            "error": None,
            "name": "Suspicious keywords",
            "weight": 50,
            "parameters": {"words":[
                                    "accrue", "accrual", "adjust", "alter", "request", "audit", "bonus", "bury",
                                    "cancel", "capital", "ceo", "cfo", "classify", "confidential", "correct",
                                    "correction", "coverup", "director", "ebit", "error", "estimate", "fix",
                                    "fraud", "gift", "hide", "incentive", "issue", "kite", "kiting", "lease",
                                    "mis", "net", "per", "plug", "problem", "profit", "reclass", "rectify",
                                    "reduce", "remove", "reverse", "reversing", "screen", "switch", "temp",
                                    "test", "transfer"
                                    ]

                },
            "count": 0,
            "sumAmount": 0
        }
       
    }

@st.cache_data(ttl=600)
def fetch_data(tablename,page, page_size,filter):
    params = {
        'page': page,
        'pageSize': page_size,
        'tablename':tablename,
        'filter':filter
        
    }
    response = requests.get(API_URL_DATA, params=params)
    if response.status_code == 200:
        result = response.json()
        columns = result['columns']
        data = result['data']
        df = pd.DataFrame(data)
        df = df[columns]
        return df
    else:
        st.error("Error fetching data")
        return pd.DataFrame()
    
@st.cache_data
def get_sas_url(file_name=None):
    params = {"container": "testcontainer"}
    # print(GENERATE_SAS_TOKEN_ENDPOINT)
    if file_name:
        params["fileName"] = file_name
    headers = {}
    # if FUNCTION_KEY:
    #     headers["x-functions-key"] = FUNCTION_KEY
    response = requests.get(GENERATE_SAS_TOKEN_ENDPOINT, params=params, headers=headers)
    if response.status_code == 200:
        return response.json()['sasUrl']
    else:
        st.error("Failed to get SAS token.")
        st.stop()

#@st.cache_data
def upload_file_to_blob( file,filename):
    blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string, max_block_size=1024*1024*4,        max_single_put_size=1024*1024*8 )

    blob_client = blob_service_client.get_blob_client(container="testcontainer", blob=filename)
    
    ret=blob_client.upload_blob(file, overwrite=True,max_concurrency=4)
    #st.success("File uploaded to Azure Blob Storage.")

    #print(ret)
    
#@st.cache_data
def start_orchestration(input_data):
    headers = {"Content-Type": "application/json"}
    # if FUNCTION_KEY:
    #     headers["x-functions-key"] = FUNCTION_KEY
    response = requests.post(START_ORCHESTRATOR_ENDPOINT, headers=headers, data=json.dumps(input_data))
    if response.status_code == 200:
        return response.json()['instanceId']
    else:
        st.error(f"Failed to start orchestration: {response.text}")
        st.stop()

def local_css(file_name):
    with open(file_name) as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)


def check_job_status(instance_id,typeid):
    headers = {}
    # if FUNCTION_KEY:
    #     headers["x-functions-key"] = FUNCTION_KEY
#    st.write(f"{CHECK_JOB_STATUS_ENDPOINT}/{instance_id}/{typeid}")
    response = requests.get(f"{CHECK_JOB_STATUS_ENDPOINT}/{instance_id}/{typeid}", headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        st.error(f"Failed to check job status: {response.text}")
        st.stop()

def update_tests(test_dest, status):
    for test_name, test_info in status.items():
        if test_name in test_dest:
            for key, value in test_info.items():
                # Update only if the value is not None or different from the existing status
                if value is not None and test_dest[test_name].get(key) != value:
                    test_dest[test_name][key] = value
    return test_dest

#@st.cache_data
def extract_columns( file_name):
    # Prepare input data for column extraction
    input_data = {
        
        
        "FileName": file_name,
        "DatabricksJobId": 447087718645534
    }

    headers = {"Content-Type": "application/json"}
    #if FUNCTION_KEY:
        #headers["x-functions-key"] = FUNCTION_KEY

    # Trigger the column extraction orchestrator
    
    response = requests.post(EXTRACT_COLUMNS_ENDPOINT, headers=headers, data=json.dumps(input_data))
    
    if response.status_code == 200:
        return response.json()['instanceId']
    else:
        st.error(f"Failed to start column extraction: {response.text}")
        st.stop()


def create_chart():
    df = st.session_state.get('df')
    if df is not None and not df.empty:
        series = [
            {
                'name': 'Benford Prediction',
                'data': df['BenfordPrediction'].tolist(),
                'type': 'line'
            },
            {
                'name': 'Sample Occurrence',
                'data': df['SampleOccurrence'].tolist(),
                'type': 'line'
            },
            {
                'name': 'Lower Limit',
                'data': df['LowerLimit'].tolist(),
                'type': 'line',
                'dashStyle': 'ShortDot'
            },
            {
                'name': 'Upper Limit',
                'data': df['UpperLimit'].tolist(),
                'type': 'line',
                'dashStyle': 'ShortDot'
            }
        ]

        options = {
            'chart': {'type': 'line'},
            'title': {'text': 'Benford Analysis'},
            'xAxis': {
                'categories': df['digit'].astype(str).tolist(),
                'title': {'text': 'Digit'}
            },
            'yAxis': {'title': {'text': 'Value'}},
            'series': series
        }

        hct.streamlit_highcharts(options)
        st.dataframe(df)
    else:
        st.error("No data to display")

def display_parameters(test_key):
    test = test_data[test_key]
    st.write("Set parameters for:", test["name"])
  
    # Add specific parameter inputs here based on test type
    if test_key == "UnusualTest":
        test["parameters"]["threshold"] = st.number_input(
            "Threshold for Unusual Account Combinations (X)", min_value=1, value=5, key=test_key + "_param_x")
    elif test_key == "OutlierTest":
        test["parameters"]["std_dev"] = st.number_input(
            "Standard Deviations for Outlier Detection (Y)", min_value=1, value=3, key=test_key + "_param_y")
    elif test_key == "RoundedTest":
        test["parameters"]["rounding_base"] = st.selectbox(
            "Select Rounding Base", [10, 100, 1000, 10000, 100000, 1000000], key=test_key + "_param_rounding")
    elif test_key == "SuspiciousTest":

        new_word = st.text_input("Enter a word to add:", key="add_word_input")
        if st.button("Add Word", key="add_word_button"):
            if new_word and new_word not in test["parameters"]["words"] :
                test["parameters"]["words"].append(new_word)
                st.success(f"Word '{new_word}' added to the list.")
            elif not new_word:
                st.warning("Please enter a valid word.")
            else:
                st.warning("Word already exists in the list.")
    
        # Section to remove a word
        st.subheader("Remove a Word")
        selected_words = st.multiselect(
            "Select words to remove:",
            options=test["parameters"]["words"],
            key="remove_word_listbox"
        )
        if st.button("Remove Selected Words", key="remove_word_button"):
            if selected_words:
                for word in selected_words:
                    if word in test["parameters"]["words"]:
                        test["parameters"]["words"].remove(word)
                st.success(f"Removed words: {', '.join(selected_words)}")
            else:
                st.warning("No words selected for removal.")

        # Display the list of words dynamically in a listbox
        st.subheader("  Suspicious Words       ")
        num_columns = 3
        rows = [test["parameters"]["words"][i:i+num_columns] for i in range(0, len(test["parameters"]["words"]), num_columns)]
        df = pd.DataFrame(rows)
        df.columns=[f"Column {i+1}" for i in range(num_columns)]
        st.dataframe(df,hide_index=True)
        

def display_tableTests():
    test_data= st.session_state['test_data']
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
    
    
    
    for test_key, test in test_data.items():
        # print("!!!!!")    
        # print(test_key)    
        # print(test)    
        if(test==0):
            continue
        
        if("name" not in test):
            continue
        
        col1, col2, col3, col4, col5, col6 = st.columns([2, 1, 2, 2, 1, 1])
        
        with col1:
            st.text(test["name"])
        with col2:
            st.text(test["status"])
        with col3:
            test["weight"] = st.slider("Weight", 1,100, test["weight"], key=test_key + "_weight",label_visibility="collapsed")
        with col4:
            with st.popover("", icon=":material/settings:"):
                display_parameters(test_key)
        with col5:
            st.text(test["count"])
        with col6:
            st.text(locale.currency(test["sumAmount"], grouping=True))
    

def poll_for_columns(test_data, polling_interval=2, max_attempts=30):
    columns = []
    # print("start 1")
    input_data = {
            
                    "ContainerName": "uploads",
                    "FileName": test_data['unique_file_name'],
                    "SelectedColumns": "",  # To be updated after column selection
                    "DatabricksJobId": 447087718645534  # Your Databricks job ID
                }
    instance_id= extract_columns(test_data['unique_file_name'])
    print("poll for coll"+instance_id)
    for _ in range(max_attempts):
        time.sleep(polling_interval)
        print("start 2")
        #st.session_state['polling_status'] = "completed"
        status = check_job_status(instance_id, "columns")
        
        if status["output"] is not None:
            
            test_data["status_column"]="Completed"
            output_data= json.loads(status["output"] )
            test_data["postedbyList"] = output_data["postedbyList"]
            print("poll_for_columns is Completed!!!!")  
            return
     #   else:
           #st.session_state['polling_status'] = "in_progress"
    
    if not columns:
        # status_queue.put( "failed")
        # st.session_state['polling_status'] = "failed"
        test_data["status_column"]="Failed"

def poll_for_chart(test_data,out_data, polling_interval=3, max_attempts=60):
    summary= []
    input_data={}
    input_data['DatabricksJobId']=989779811879952 #Chart
    input_data['FileName']=test_data['unique_file_name']
    input_data['Params']= test_data
    del test_data['summary']

    print("Start poll_for_chart")        
    print(input_data)
    instance_id = start_orchestration( input_data)
    print("Start poll_for_chart"+instance_id )        
    for _ in range(max_attempts):
        time.sleep(polling_interval)
        
        status = check_job_status(instance_id,"summary")
        print("poll_for_chart:"+str(status))
        #print("poll_for_task2:"+type(status))
        
        print(status)
        if 'log' in status:
            log_json = status['log']
            print("LOG:"+str(log_json) )
            if(log_json):
                
                update_tests(test_data,json.loads(log_json))
            
        if status["output"] is not None:
            print("Chart Complted !")
            
            outp=json.loads(status['output'])

            out_data['summary']= outp
            
            #out_data['summary']= status["output"]["RunTasks_Main"]
            test_data["status"]="completed"
            print("Completed poll_for_task")
            
            break
        else:
            test_data["status"]="in_progress"

def poll_for_task(test_data,out_data, polling_interval=3, max_attempts=60):
    summary= []
    input_data={}
    input_data['DatabricksJobId']=861358873659712 #BenfordRun
    input_data['FileName']=test_data['unique_file_name']
    input_data['Params']= json.dumps(test_data)
    print("Start poll_for_task")        
    instance_id = start_orchestration( input_data)
    print("Start poll_for_task"+instance_id )        
    for _ in range(max_attempts):
        time.sleep(polling_interval)
        
        status = check_job_status(instance_id,"summary")
        print("poll_for_task:"+str(status))
        #print("poll_for_task2:"+type(status))
        
        print(status)
        if 'log' in status:
            log_json = status['log']
            print("LOG:"+str(log_json) )
            if(log_json):
                
                update_tests(test_data,json.loads(log_json))
            
        if status["output"] is not None:
            print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            # Parse the 'columns' field, which is a JSON string
            outp=json.loads(status['output'])

            out_data['summary']= outp
            
            print("Summary output!!!!")
            print(out_data['summary'])
            print("Log output!!!!")
            print(outp['log'])
            
            update_tests(test_data,outp['log'])
            test_data["status"]="completed"
            print("Completed poll_for_task")
            
            break
        else:
            test_data["status"]="in_progress"
        

    
    if not summary:
        test_data["status"]="failed"
        


def init():
    if 'runbutton_enabled' not in st.session_state:
        st.session_state['runbutton_enabled'] = False
     
        
    if 'filtered_df' not in st.session_state:
        st.session_state['filtered_df']=[]  
    if  'out_data' not in st.session_state:
        st.session_state['out_data']={}
    if  'test_data' not in st.session_state:
        st.session_state['test_data']=test_data
        st.session_state['test_data']['status_column']="Not started"
        st.session_state['test_data']['status']="Not started"
        #placeholder= st.empty()
        
    if st.session_state['test_data']["status_column"]=="Completed":
            st.success("Column extraction completed.")
            st.session_state["columns"]=2
            st.session_state['runbutton_enabled'] = True
            st.session_state['col_status']="Completed"
    if  st.session_state['test_data']["status_column"]== "failed":
            st.error("Failed to extract columns.")
    if st.session_state['test_data']["status_column"]== "in_progress":
            st.info("Column extraction is still in progress...")
@st.cache_data    
def load_data_from_blob(sas_url):
    #return pd.read_parquet(sas_url)
    # print("------111:"+str(sas_url))
    return pd.read_csv(sas_url)

def createChart():
    left_col, right_col = st.columns([20,100])
    with left_col:
        with st.expander("Filters", expanded=True):
            account_df= fetch_data("accounts_type",1, 1,"")
            
            account_type = st.multiselect("Filter by Account Type", options=account_df["accountType"].unique(), default=None)
            subtype = st.multiselect("Filter by Subtype", options=account_df["AccountSubType"].unique(), default=None)

            filtered_df = account_df
            if account_type:
                filtered_df = filtered_df[filtered_df["accountType"].isin(account_type)]
            if subtype:
                filtered_df = filtered_df[filtered_df["AccountSubType"].isin(subtype)]

            st.session_state["filtered_df"]=filtered_df["id"].to_json()   
            st.session_state['test_data']["filtered_df"]=filtered_df["id"].to_json()
            #st.dataframe(filtered_df)
            if st.button("Apply"):
               
               
                thread = threading.Thread(
                    target=poll_for_chart,
                    args=(st.session_state['test_data'],st.session_state['out_data'])                )
                thread.start()
                with st.spinner("Waiting for task to complete..."):
                    while thread.is_alive():  # Check if the thread is still running
                        time.sleep(1)  # Adjust the
                    st.rerun()
                
        
                
    with right_col:
        if( 'summary' in st.session_state['out_data']):
            createChart1(st.session_state['out_data'])
            createChart2(st.session_state['out_data'])
            createChart3(st.session_state['out_data'])

def createChart1(out_data):
    

#    test_data=st.session_state['test_data']
    print(out_data)
    chart1url= out_data['summary']['chart1url']
    data = load_data_from_blob(chart1url)
    df = pd.DataFrame(data)
    
    # Create charts using HighCharts
    st.subheader("Visualization 1: High-Risk Journals Per Month")
    line_chart_config = {
    'chart': {'type': 'line'},
    'title': {'text': 'High-Risk Journals Per Month'},
    'xAxis': {
        'categories': df['month'].tolist()
    },
    'yAxis': {
        'title': {'text': 'Count of High-Risk Journals'}
    },
    'series': [{
        'name': 'High Risk',
        'data': df['high_risk_count'].tolist()
    }]
    }
    st.subheader("High-Risk Journals Per Month")
    #st.highcharts(line_chart_config)
    hct.streamlit_highcharts(line_chart_config)

def createChart2(out_data ):
    ##test_data=st.session_state['test_data']
    # print("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
    # print(test_data)
    chart1url= out_data['summary']['chart2url']
    print(chart1url )
    data = load_data_from_blob(chart1url)
    risk_per_account_df = pd.DataFrame(data)

    # Ensure the data type of 'risk_count' is a native Python int
    risk_per_account_df['risk_count'] = risk_per_account_df['risk_count'].astype(int)

    # Prepare data for HighCharts
    categories = sorted(risk_per_account_df['glAccountNumber'].unique().tolist())  # Sorted order for accounts
    risks = sorted(risk_per_account_df['risk_label'].unique().tolist())  # Sorted risks

    # Create series data for HighCharts
    series = []
    for risk in risks:
        data = []
        for account in categories:
            # Sum up risk_count for each account and risk
            count = risk_per_account_df[
                (risk_per_account_df['glAccountNumber'] == account) & (risk_per_account_df['risk_label'] == risk)
            ]['risk_count'].sum()
            data.append(int(count))  # Ensure native Python int
        series.append({'name': risk, 'data': data})

    # HighCharts configuration
    chart_options = {
        'chart': {
            'type': 'column'
        },
        'title': {
            'text': 'Risk Journals per Account'
        },
        'xAxis': {
            'categories': categories,  # Accounts on the x-axis
            'title': {
                'text': 'Accounts'
            }
        },
        'yAxis': {
            'min': 0,
            'title': {
                'text': 'Risk Count',
                'align': 'high'
            }
        },
        'plotOptions': {
            'column': {
                'stacking': 'normal'  # Enable stacking
            }
        },
        'series': series
    }

    # Render the chart in Streamlit using streamlit_highcharts
    st.write("### Stacked Column Chart: Risk Journals per Account")
    hct.streamlit_highcharts(chart_options)


def createChart3(out_data):
    
    #test_data=st.session_state['test_data']
    
    chart1url= out_data['summary']['chart3url']
    # print(chart1url)
    data = load_data_from_blob(chart1url)
    df = pd.DataFrame(data)
    
    # Highcharts configuration for pie chart
    # Prepare the data for Highcharts
    pie_data = df.to_dict(orient='records')
    # If needed, ensure proper types:
    pie_data = [{'name': str(record['risk_label']), 'y': int(record['overall_risk_count'])} for record in pie_data]

    #pie_data = [{'name': str(row['risk']), 'y': int(row['overall_risk_count'])} for row in df]

    # Highcharts configuration for pie chart
    chart_config = {
        'chart': {
            'type': 'pie'
        },
        'title': {
            'text': 'Overall Number of Low/Medium/High Risk Journals'
        },
        'series': [{
            'name': 'Journals Count',
            'colorByPoint': True,
            'data': pie_data
        }]
    }

    # Render the chart in Streamlit
    st.title("Risk Distribution Pie Chart")
    
    hct.streamlit_highcharts(chart_config)


        
def main():

    # Page Configuration
    st.set_page_config(page_title="General Ledger testing ", layout="wide")
    local_css("style.css")    
    st.markdown("<img src='https://cdn.wolterskluwer.io/wk/fundamentals/1.15.2/logo/assets/medium.svg' alt='Wolters Kluwer Logo' width='190px' height='31px'>", unsafe_allow_html=True)
    st.title("General Ledger testing v"+version)
    init()
    
    uploaded_file = st.file_uploader("Choose a CSV,ZIP file", type=['zip', 'csv'] )
    col1, col2,col3 = st.columns([1, 1,1])
    with col1:
        exccel_clicked = st.button('Excel', disabled=not st.session_state['runbutton_enabled'],use_container_width=True)
    with col2:
        createchart_clicked = st.button('Report', disabled=not st.session_state['runbutton_enabled'],use_container_width=True)
    with col3:
        runbutton_clicked = st.button('Run tests', disabled=not st.session_state['runbutton_enabled'],use_container_width=True)
    #createChart()        

    if runbutton_clicked:
        
        thread = threading.Thread(
                target=poll_for_task,
                args=(st.session_state['test_data'],st.session_state['out_data'])                )
        thread.start()
        with st.spinner("Waiting for task to complete..."):
            while thread.is_alive():  # Check if the thread is still running
                time.sleep(1)  # Adjust the
        thread=None
    
    

    if uploaded_file:
        if 'columns' not in st.session_state:
            original_file_name = uploaded_file.name
            
            
            if 'fileUploaded' not in st.session_state:
                with st.spinner("Uploading file..."):
                    #unique_file_name = f"{uuid.uuid4()}{original_file_name}"
                    unique_file_name = f"poc{original_file_name}"
                    st.session_state['test_data']['unique_file_name']=unique_file_name
                    supload_url = upload_file_to_blob( uploaded_file,unique_file_name)
                    st.session_state['fileUploaded']=True
                    
    
        
        if 'col_status' not in st.session_state :
            
            st.session_state['col_status']="in progress"
            st.session_state['test_data']['status_column']="in progess"
            st.session_state['threadUpload']= threading.Thread(                target=poll_for_columns,                args=(st.session_state['test_data'],)                )       
            st.session_state['threadUpload'].start()
            
            
            
        
        if(st.session_state['test_data']['status_column']=="Completed"):
            st.session_state['runbutton_enabled']=True
        if(st.session_state['test_data']['status_column']=="in progess"):
            st.info("CSV extraction is running in the background. You can continue using the UI.")
        if(st.session_state['test_data']['status_column']=="Failed"):
            st.error("Isssue in uploading")

    
    if( "postedbyList" in  st.session_state['test_data']):
        st.subheader("Select posted by") 
        st.session_state['test_data']['selectPostedBy' ]= st.multiselect(            "",            options=st.session_state['test_data']["postedbyList"],            key="postedbyList",placeholder="All items"        )
        
    st.subheader("Select Tests to Run")
       

    display_tableTests()
       
     
    tab1, tab2 = st.tabs(["📈 Chart", "🗃 Tables"])

    with tab1:
        
        if(st.session_state['out_data']):
                createChart()
                    
    with tab2:
        if 'summary' in st.session_state['out_data']:
            main2(st.session_state['test_data'],st.session_state['out_data'])
    st.button(".")                 
    if 'threadUpload'  in st.session_state:
        if st.session_state['threadUpload'].is_alive():  # Check if the thread is still running
            print("Thread uploading")
            time.sleep(1)  # Adjust the
            st.rerun() 
                                    



if __name__ == "__main__":

    main()
    #create_chart()
        
