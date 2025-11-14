import uuid
import time
import json
from io import BytesIO

import streamlit as st
import requests
from azure.storage.blob import BlobServiceClient

FUNCTION_BASE_URL = "http://localhost:7190/api"  # or your Function App URL
version = "0.95a"

GENERATE_SAS_TOKEN_ENDPOINT = f"{FUNCTION_BASE_URL}/GenerateSASToken"
START_ORCHESTRATOR_ENDPOINT = f"{FUNCTION_BASE_URL}/start-orchestrator"

API_URL_DATA = f"{FUNCTION_BASE_URL}/GetPaginatedData"
API_URL_DOWNLOAD = f"{FUNCTION_BASE_URL}/DownloadTableCsv"

storage_connection_string = (
    "DefaultEndpointsProtocol=https;"
    "AccountName=zuscutaargpletoaudi9020;"
    "AccountKey=i2Fs+bpmHyCWzk/lwpkclGW6gWaGQumksWbQgjDmverFwG+O/"
    "lmz1aTTvHxawzyT+rRDfxw3DKQ9+ASt8RFXow==;"
    "EndpointSuffix=core.windows.net"
)
storage_account = "zuscutaargpletoaudi9020"
container = "testcontainer"

FUNCTION_KEY = ""  # if you use function keys, put it here


# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------
def get_function_headers() -> dict:
    headers = {"Content-Type": "application/json"}
    if FUNCTION_KEY:
        headers["x-functions-key"] = FUNCTION_KEY
    return headers


def upload_file_to_blob(file, filename: str) -> str:
    """
    Upload file to Azure Blob Storage and return the blob URL.
    """
    blob_service_client = BlobServiceClient.from_connection_string(
        storage_connection_string,
        max_block_size=1024 * 1024 * 4,
        max_single_put_size=1024 * 1024 * 8,
    )

    blob_client = blob_service_client.get_blob_client(
        container=container,
        blob=filename,
    )

    blob_client.upload_blob(file, overwrite=True, max_concurrency=4)

    blob_url = blob_client.url
    st.success(f"File uploaded to Azure Blob Storage:\n{blob_url}")
    return blob_url


def get_sas_url_for_blob(blob_name: str) -> str:
    """
    Call Azure Function to generate SAS URL for the uploaded blob.
    Adjust the payload according to your function contract.
    """
    payload = {
        "storageAccount": storage_account,
        "containerName": container,
        "blobName": blob_name,
    }

    response = requests.post(
        GENERATE_SAS_TOKEN_ENDPOINT,
        headers=get_function_headers(),
        json=payload,
    )
    response.raise_for_status()
    data = response.json()

    # Adjust key name if your function returns something different
    sas_url = data.get("sasUrl") or data.get("sasURL") or data.get("url")
    if not sas_url:
        raise ValueError(f"GenerateSASToken did not return SAS URL. Response: {data}")

    return sas_url


def start_orchestrator_with_blob_url(blob_url: str) -> dict:
    """
    Start the orchestrator (or main processing) by POSTing blob URL to your function.
    Adjust the payload field names to match your Function definition.
    """
    payload = {
        "blobUrl": blob_url,  # field name must match what your Function expects
        # add any extra parameters here if needed
        # "someParam": "value"
    }

    response = requests.post(
        START_ORCHESTRATOR_ENDPOINT,
        headers=get_function_headers(),
        json=payload,
    )
    response.raise_for_status()
    return response.json()


# -------------------------------------------------------------------
# STREAMLIT APP
# -------------------------------------------------------------------
def local_css(file_name: str):
    try:
        with open(file_name) as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    except FileNotFoundError:
        pass


def main():
    st.set_page_config(page_title="General Ledger testing", layout="wide")
    local_css("style.css")

    st.markdown(
        "<img src='https://cdn.wolterskluwer.io/wk/fundamentals/1.15.2/logo/assets/medium.svg' "
        "alt='Wolters Kluwer Logo' width='190px' height='31px'>",
        unsafe_allow_html=True,
    )
    st.title(f"General Ledger testing v{version}")

    # File upload
    uploaded_file = st.file_uploader(
        "STEP 2: Choose General Ledger file",
        type=["zip", "csv"],
        key="x2",
    )

    runbutton_clicked = st.button("Run tests", use_container_width=True)

    if uploaded_file is not None:
        # Show basic info
        st.info(f"Selected file: {uploaded_file.name}")

        # Use deterministic name or add GUID – up to you
        original_file_name = uploaded_file.name
        unique_file_name = f"ca-{uuid.uuid4()}-{original_file_name}"

        # Upload file immediately when chosen
        with st.spinner("Uploading file to Blob Storage..."):
            blob_url = upload_file_to_blob(uploaded_file, unique_file_name)

        # Generate SAS URL for that blob (if your backend needs SAS)
        with st.spinner("Generating SAS URL..."):
            sas_url = get_sas_url_for_blob(unique_file_name)
        st.write("SAS URL (for debugging):")
        st.code(sas_url)

        # Save for later use (e.g., other pages / callbacks)
        st.session_state["uploaded_blob_name"] = unique_file_name
        st.session_state["uploaded_blob_sas_url"] = sas_url

        # Only start orchestrator when user clicks Run tests
        if runbutton_clicked:
            with st.spinner("Starting orchestrator with blob URL..."):
                try:
                    result = start_orchestrator_with_blob_url(sas_url)
                    st.success("Orchestrator started successfully.")
                    st.json(result)
                except Exception as ex:
                    st.error(f"Failed to start orchestrator: {ex}")

    else:
        st.info("Please upload a General Ledger file (.zip or .csv).")


if __name__ == "__main__":
    main()
