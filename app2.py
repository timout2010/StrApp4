# Azure Function endpoints
#FUNCTION_BASE_URL = "http://localhost:7190/api" # e.g., https://<function-app>.azurewebsites.net/api/
FUNCTION_BASE_URL = "https://alexfuncdoc.azurewebsites.net/api" # e.g., https://<function-app>.azurewebsites.net/api/

API_URL_DATA = f"{FUNCTION_BASE_URL}/GetPaginatedData"
API_URL_TOTAL_RECORDS = f"{FUNCTION_BASE_URL}/GetTotalRecords"
API_URL_SUBTABLE = f"{FUNCTION_BASE_URL}/GetSubtableData"


test_data2 = {
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
        }
    }


# Azure Function endpoints

#from symbol import test_nocond
import streamlit as st
import requests
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import json
import math

# Constants
PAGE_SIZE_DEFAULT = 100
def local_css(file_name):
    with open(file_name) as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)


def remote_css(url):
    st.markdown(f'<link href="{url}" rel="stylesheet">',
                unsafe_allow_html=True)


def header_bg(table_type):
    if table_type == "BASE TABLE":
        return "tablebackground"
    elif table_type == "VIEW":
        return "viewbackground"
    else:
        return "mvbackground"

def get_risk_class(risk_level):
    if risk_level == "LOW":
        return "low-risk"
    elif risk_level == "MEDIUM":
        return "medium-risk"
    elif risk_level == "HIGH":
        return "high-risk"
    else:
        return "no-risk"


@st.cache_data(ttl=3600)
def get_total_records(tablename,filter):
    params = {
        'filter':filter,
        'tablename':tablename
        
    }
    response = requests.get(API_URL_TOTAL_RECORDS,params)
    if response.status_code == 200:
        data = response.json()
        total_records = data['totalRecords']
        return total_records
    else:
        st.error("Error fetching total records")
        return 0

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

def fetch_subtable_data(tablename,filter,journal_id,posted_date,posted_by):
    params = {
        'journal_id': journal_id,
        'posted_date': posted_date,
        'posted_by': posted_by,
        'tablename':tablename,
        'filter':filter
    }
    response = requests.get(API_URL_SUBTABLE, params=params)
    if response.status_code == 200:
        result = response.json()
        columns = result['columns']
        data = result['data']
        df = pd.DataFrame(data)
        df = df[columns]
        return df
    else:
        st.error("Error fetching subtable data")
        return pd.DataFrame()
def load_data_from_blob(sas_url):
    #return pd.read_parquet(sas_url)

    return pd.read_csv(sas_url)
def sanitize_table_name(name):
    """Sanitizes a string to be used as a valid table name by replacing or removing special characters."""
    return name.replace(' ', '_').replace(';', '').replace('(', '').replace(')', '').replace(',', '').replace('.', '').replace('-', '')

def main2(test_data,out_data):
    #st.set_page_config(page_title="General Ledger testing", layout="wide")
    remote_css(   "https://cdnjs.cloudflare.com/ajax/libs/semantic-ui/2.4.1/semantic.min.css")

    if test_data is None:
        test_data =test_data2

   # local_css("style.css")
    tablename=sanitize_table_name(test_data['unique_file_name'])

#    test_data =test_data
    st.title("Result screen - charts")
    chart3url= out_data['summary']['chart3url']
    
    
    data = load_data_from_blob(chart3url)
    df = pd.DataFrame(data)
    table_scorecard=""

    num_cols = 3
    cards_per_row = num_cols

    # Open the main container div
    total_cards = len(df)
    total_rows = math.ceil(total_cards / cards_per_row)

# Create a container div for the cards
    

    # Iterate over the DataFrame in chunks of three
    for row_num in range(total_rows):
        # Create a set of columns for the current row
        cols = st.columns(num_cols)
    
        for col_num in range(num_cols):
            # Calculate the index of the card
            idx = row_num * cards_per_row + col_num
        
            if idx < total_cards:
                row_data = df.iloc[idx]
                with cols[col_num]:
                    # Render the card HTML
                    st.markdown(f"""
                        <div class="card">
                            <div class="{get_risk_class(row_data['risk_label'])}">
                                <div class="header">{row_data['risk_label']}</div>
                                <div class="meta">Risk Level</div>
                            </div>
                            <div class="kpi">
                                <div class="metric">
                                    <div class="number">{row_data['overall_risk_count']}</div>
                                    <div class="label">Total Journals</div>
                                </div>
                                <div class="metric">
                                    <div class="number">${row_data['sum_amount']:,.2f}</div>
                                    <div class="label">Total Debit Amount </div>
                                </div>
                            </div>
                            <div class="full-width-button">
                                <!-- Streamlit button will be rendered here -->
                            </div>
                        </div>
                    """, unsafe_allow_html=True)
                
                    # Add a button below the card with a unique key
                    if st.button("View", key=f"action_{idx}"):
                        st.session_state.page = 1
                        st.session_state.filter=row_data['risk_label']

    
                        
                        
    
    num_cols = 3
    cards_per_row = num_cols

    # Open the main container div
    total_cards = len(test_data)
    total_rows = math.ceil(total_cards / cards_per_row)

# Create a container div for the cards
    

    # Iterate over the DataFrame in chunks of three
#    for row_num in range(total_rows):
        # Create a set of columns for the current row
    
    num_cols = 5
    cards_per_row=5
    col_num=-1
    row_num =-1
    cols = st.columns(num_cols)
    
    for test_key, row_data in test_data.items():
        if("name" not in row_data ):
                continue
        col_num+=1
        if col_num>=cards_per_row :
            col_num=0
            row_num +=1
        # Calculate the index of the card
        idx = row_num * cards_per_row + col_num
        
        if idx < total_cards:
            
            with cols[col_num]:
                # Render the card HTML
                st.markdown(f"""
                    <div class="card">
                        <div class="{get_risk_class("d")}">
                            <div class="header">{row_data['name']}</div>
                            <div class="meta">Risk Level</div>
                        </div>
                        <div class="kpi">
                            <div class="metric">
                                <div class="number">{row_data['count']}</div>
                                <div class="label">Total Journals</div>
                            </div>
                            <div class="metric">
                                <div class="number">${row_data['sumAmount']:,.2f}</div>
                                <div class="label">Total Debit Amount </div>
                            </div>
                        </div>
                        <div class="full-width-button">
                            <!-- Streamlit button will be rendered here -->
                        </div>
                    </div>
                """, unsafe_allow_html=True)
                
                # Add a button below the card with a unique key
                if st.button("View", key=f"action2_{idx}"):
                    st.session_state.page = 1
                    st.session_state.filter=test_key
                    


    page_size =100
    # Initialize session state for page number
    if 'page' not in st.session_state:
        st.session_state.page = 1
    if 'filter' not in st.session_state:
        st.session_state.filter="none"
    
    # Get total number of records
    total_records = get_total_records(tablename,st.session_state.filter)
    print(st.session_state.filter)
    print(total_records )
    if total_records == 0:
        st.stop()

    total_pages = (total_records + page_size - 1) // page_size

    
        

    # Fetch and display data using AgGrid
    data = fetch_data(tablename,st.session_state.page, page_size,st.session_state.filter)
    st.markdown(f"### {st.session_state.filter}")
    st.write(f"Displaying page {st.session_state.page} of {total_pages} (Total records: {total_records}) ")
    
    # Configure AgGrid options
    gb = GridOptionsBuilder.from_dataframe(data)
    gb.configure_default_column(filterable=True, sortable=True, resizable=True)
    gb.configure_selection(selection_mode='single', use_checkbox=False)
    grid_options = gb.build()

    # Display data using AgGrid
    grid_response = AgGrid(
        data,
        gridOptions=grid_options,
        height=400,
        width='100%',
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        data_return_mode='FILTERED',
        fit_columns_on_grid_load=True
    )

    # Pagination controls under the table
    
    pagination_container = st.container()
    with pagination_container:
        cols = st.columns(5)

        # 'First' button
        if st.session_state.page > 1:
            if cols[0].button("⏮ First"):
                st.session_state.page = 1
                st.rerun()
        else:
            cols[0].write("")

        # 'Previous' button
        if st.session_state.page > 1:
            if cols[1].button("◀ Previous"):
                st.session_state.page -= 1
                st.rerun()
        else:
            cols[1].write("")

        # Page number input
        page_input = cols[2].number_input(
            "Page",
            min_value=1,
            max_value=total_pages,
            value=st.session_state.page,
            key='page_input',
            label_visibility="collapsed"
        )
        if page_input != st.session_state.page:
            st.session_state.page = page_input
            st.rerun()

        # 'Next' button
        if st.session_state.page < total_pages:
            if cols[3].button("Next ▶"):
                st.session_state.page += 1
                st.rerun()
        else:
            cols[3].write("")

        # 'Last' button
        if st.session_state.page < total_pages:
            if cols[4].button("Last ⏭"):
                st.session_state.page = total_pages
                st.rerun()
        else:
            cols[4].write("")

    # Handle row selection and display subtable
    selected_rows = grid_response['selected_rows']
    if selected_rows is not None:
        #st.write(selected_rows.get('journal_id'))
        selected_row = selected_rows
        journal_id= selected_row.get('journalid')  # Adjust 'id' to the column name that identifies the selected item
        posted_date= selected_row.get('enteredDateTime')  # Adjust 'id' to the column name that identifies the selected item
        posted_by= selected_row.get('enteredBy')  # Adjust 'id' to the column name that identifies the selected item
        if journal_id is not None:
            st.markdown(f"### journal_id:{journal_id.iloc[0]}")
            with st.spinner("Loading.."):
                subtable_data = fetch_subtable_data(tablename,st.session_state.filter,journal_id,posted_date,posted_by)
            if not subtable_data.empty:
                st.dataframe(subtable_data)
            else:
                st.write("No data available for the selected item.")
        else:
            st.error("Selected row does not contain 'id' column.")
    else:
        st.write("Select a row to see related data in the subtable.")

if __name__ == "__main__":
    main2(None)
