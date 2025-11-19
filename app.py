import uuid
from io import BytesIO

import streamlit as st
import requests
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import (
    BlobServiceClient,
    BlobSasPermissions,
    generate_blob_sas
)
from datetime import datetime, timedelta
# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------
# Azure Storage
storage_connection_string = "DefaultEndpointsProtocol=https;AccountName=zusculetoaudstrdda01;AccountKey=l3PYt2ZrVnGDODnzJRwAD6bL8QXmQdKluC+gTYOLB7FwkQNp8RqSMfEJbaaRW5na7ZI8SnZyC96X+ASteh11FQ==;EndpointSuffix=core.windows.net"
storage_account = "zuscutaargpletoaudi9020"
container = "testcontainer"

# Your C# API endpoint
#PREVIEW_API_URL = "http://localhost:7164/api/Preview"
PREVIEW_API_URL = "https://automappoc.azurewebsites.net/api/Preview"
version = "0.1a"


# ---------------------------------------------------
# HELPERS
# ---------------------------------------------------
def local_css(file_name: str):
    try:
        with open(file_name) as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    except FileNotFoundError:
        pass


def upload_file_to_blob(file, filename: str) -> str:
    """
    Uploads a file to Azure Blob Storage and returns a SAS URL.
    """

    # --- init blob client ---
    blob_service = BlobServiceClient.from_connection_string(storage_connection_string)
    blob_client = blob_service.get_blob_client(container=container, blob=filename)

    # --- upload ---
    blob_client.upload_blob(file, overwrite=True, max_concurrency=4)

    # --- generate SAS token ---
    sas_token = generate_blob_sas(
        account_name=blob_service.account_name,
        container_name=container,
        blob_name=filename,
        account_key=blob_service.credential.account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=3)  # adjust as needed
    )

    sas_url = f"{blob_client.url}?{sas_token}"

    st.success("File uploaded. SAS URL generated.")
    return sas_url,filename

def call_preview_api(
    blob_url: str,
    prefix_w: float,
    balance_w: float,
    token_w: float,
    substring_w: float,
    pre_llm_threshold: float | None,
    merge_alpha: float | None,
    top_k: int | None,
):
    """
    Builds AutoMapPreviewRequest and sends POST to /api/preview.
    C# models:

        public class AutoMapPreviewRequest
        {
            public Weights? Weights { get; set; }
            public double? PreLlmThreshold { get; set; }
            public double? MergeAlpha { get; set; }
            public int? TopK { get; set; }
            public string blobUrl { get; set; } = "";
        }

        public class Weights
        {
            public double Prefix { get; set; } = 0.30;
            public double Balance { get; set; } = 0.25;
            public double Token { get; set; } = 0.25;
            public double Substring { get; set; } = 0.20;
        }
    """

    # JSON shape matches C# contract (camelCase is fine, ASP.NET is case-insensitive)
    payload = {
        "weights": {
            "prefix": prefix_w,
            "balance": balance_w,
            "token": token_w,
            "substring": substring_w,
        },
        "preLlmThreshold": pre_llm_threshold,
        "mergeAlpha": merge_alpha,
        "topK": top_k,
        "blobUrl": blob_url,
    }

    headers = {"Content-Type": "application/json"}

    response = requests.post(PREVIEW_API_URL, json=payload, headers=headers, verify=False)
    # NOTE: verify=False only for local dev with self-signed HTTPS; remove in prod.
    response.raise_for_status()
    return response.json()

def delete_blob(blob_name: str):
    """
    Delete a blob by name
    """
    blob_service = BlobServiceClient.from_connection_string(storage_connection_string)
    blob_client = blob_service.get_blob_client(container=container, blob=blob_name)

    try:
        blob_client.delete_blob()
        st.info(f"Blob deleted: {blob_name}")
    except Exception as ex:
        st.warning(f"Failed to delete blob {blob_name}: {ex}")

# ---------------------------------------------------
# STREAMLIT UI
# ---------------------------------------------------
def main():
    st.set_page_config(page_title="Auto mapp testing", layout="wide")
    local_css("style.css")

    st.markdown(
        "<img src='https://cdn.wolterskluwer.io/wk/fundamentals/1.15.2/logo/assets/medium.svg' "
        "alt='Wolters Kluwer Logo' width='190px' height='31px'>",
        unsafe_allow_html=True,
    )
    st.title(f"AutoMap  v{version}")

    # --- Step 1: File upload ---
    st.subheader("Step 1: Upload the Trial Balance file. The Excel file must contain exactly the following columns: A: accountNumber, B: accountName, C: description, D: amount.")
    uploaded_file = st.file_uploader(
        "Choose a Trial balance file (.xlsx)",
        type=["xls", "xlsx"],
        key="gl_file",
    )

    # --- Step 2: AutoMapPreview tunables ---
    st.subheader("Step 2: AutoMapPreview Parameters")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Weights** (must ideally sum to 1.0)")
        prefix_w = st.number_input("Prefix weight", min_value=0.0, max_value=1.0, value=0.30, step=0.01)
        balance_w = st.number_input("Balance weight", min_value=0.0, max_value=1.0, value=0.25, step=0.01)
        token_w = st.number_input("Token weight", min_value=0.0, max_value=1.0, value=0.25, step=0.01)
        substring_w = st.number_input("Substring weight", min_value=0.0, max_value=1.0, value=0.20, step=0.01)

    with col2:
        pre_llm_threshold = st.number_input(
            "Pre LLM threshold",
            min_value=0.0,
            max_value=1.0,
            value=0.81,
            step=0.01,
            format="%.2f",
        )
        merge_alpha = st.number_input(
            "Merge alpha",
            min_value=0.0,
            max_value=1.0,
            value=0.5,
            step=0.01,
            format="%.2f",
        )
        top_k = st.number_input(
            "TopK candidates",
            min_value=1,
            max_value=500,
            value=250,
            step=1,
        )

    st.markdown("---")

    # --- Run button ---
    runbutton_clicked = st.button("Run process", use_container_width=True)

    # Handle logic when button clicked
    if runbutton_clicked:
        if uploaded_file is None:
            st.error("Please upload a TB file before running tests.")
            return

        # Upload file to blob (if not already uploaded)
        original_file_name = uploaded_file.name
        unique_file_name = f"ca-{uuid.uuid4()}-{original_file_name}"

        with st.spinner("Uploading file to Azure Blob Storage..."):
            blob_url,blob_name = upload_file_to_blob(uploaded_file, unique_file_name)

        # You can also display / override blobUrl manually if needed
        st.write("Using blobUrl:")
        st.code(blob_url, language="text")

        # Call your preview API
        with st.spinner("Calling AutoMap preview API..."):
            try:
                result = call_preview_api(
                    blob_url=blob_url,
                    prefix_w=prefix_w,
                    balance_w=balance_w,
                    token_w=token_w,
                    substring_w=substring_w,
                    pre_llm_threshold=pre_llm_threshold,
                    merge_alpha=merge_alpha,
                    top_k=top_k,
                )
		
                show_preview_response(result)

                st.success(f"Preview API call succeeded")
                #st.json(result)
            except Exception as ex:
                st.error(f"Failed to call preview API: {ex}")
            finally:
	            # 3. ALWAYS delete uploaded blob (success or fail)
                delete_blob(blob_name)


import pandas as pd
import streamlit as st

def show_preview_response(resp: dict):
    """
    Display AutoMapPreviewResponse as a single flat table.

    AutoMapPreviewResponse:
      - EngagementId / engagementId
      - GroupingListId / groupingListId
      - Rows / rows: list of PreviewRow

    PreviewRow fields:
      AccountNumber, AccountName, Amount, BalanceType,
      Type, Classification, Group, Subgroup,
      ConfidenceScore, ConfidenceBand, MappingRationale,
      Signals: { PrefixMatchScore, BalanceDirectionScore, TokenOverlapScore, SubstringScore }
    """

    # Handle camelCase / PascalCase keys from ASP.NET
    engagement_id = resp.get("engagementId") or resp.get("EngagementId") or ""
    grouping_list_id = resp.get("groupingListId") or resp.get("GroupingListId") or ""
    rows = resp.get("rows") or resp.get("Rows") or []

    if not rows:
        st.warning("No rows returned in preview response.")
        return

    flat_rows = []

    for r in rows:
        signals = r.get("signals") or r.get("Signals") or {}

        flat_rows.append({
    
            "AccountNumber": r.get("accountNumber") or r.get("AccountNumber"),
            "AccountName": r.get("accountName") or r.get("AccountName"),
            "Description": r.get("description") or r.get("Description"),
            "Amount": r.get("amount") or r.get("Amount"),
            "BalanceType": r.get("balanceType") or r.get("BalanceType"),
            "LLM": r.get("LLM") or r.get("llm"),

            "Type": r.get("type") or r.get("Type"),
            "Classification": r.get("classification") or r.get("Classification"),
            "Group": r.get("group") or r.get("Group"),
            
            

            "ConfidenceScore": r.get("confidenceScore") or r.get("ConfidenceScore"),
            "ConfidenceBand": r.get("confidenceBand") or r.get("ConfidenceBand"),
            "MappingRationale": r.get("mappingRationale") or r.get("MappingRationale"),

            "PrefixMatchScore": signals.get("prefixMatchScore") or signals.get("PrefixMatchScore"),
            "BalanceDirectionScore": signals.get("balanceDirectionScore") or signals.get("BalanceDirectionScore"),
            "TokenOverlapScore": signals.get("tokenOverlapScore") or signals.get("TokenOverlapScore"),
            "SubstringScore": signals.get("substringScore") or signals.get("SubstringScore"),
        })

    df = pd.DataFrame(flat_rows)

    st.subheader("AutoMap Preview Result")
    st.dataframe(df, use_container_width=True)




if __name__ == "__main__":
    main()
