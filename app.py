import os
import json
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st
from azure.storage.blob import BlobSasPermissions, BlobServiceClient, generate_blob_sas

# ---------------------------------------------------
# APP VERSION
# ---------------------------------------------------
VERSION = "0.8.1"

# ---------------------------------------------------
# CONFIG HELPERS
# ---------------------------------------------------
def get_secret(name: str, default: Optional[str] = None) -> Optional[str]:
    """Read config from Streamlit secrets first, then environment variables."""

    return default


def normalize_base_url(base_url: str) -> str:
    return base_url.rstrip("/")


# ---------------------------------------------------
# DEFAULT CONFIG
# ---------------------------------------------------
#DEFAULT_API_BASE_URL = get_secret("AUTOMAP_API_BASE_URL", "http://localhost:7164")
#DEFAULT_PREVIEW_PATH = get_secret("AUTOMAP_PREVIEW_PATH", "/api/Preview2")
DEFAULT_CONTAINER = get_secret("AZURE_STORAGE_CONTAINER", "testcontainer")
DEFAULT_STORAGE_CONNECTION_STRING = get_secret("AZURE_STORAGE_CONNECTION_STRING", "DefaultEndpointsProtocol=https;AccountName=zusculetoaudstrdda01;AccountKey=l3PYt2ZrVnGDODnzJRwAD6bL8QXmQdKluC+gTYOLB7FwkQNp8RqSMfEJbaaRW5na7ZI8SnZyC96X+ASteh11FQ==;EndpointSuffix=core.windows.net")

DEFAULT_API_BASE_URL = "https://automappoc.azurewebsites.net"
DEFAULT_PREVIEW_PATH = "https://automappoc.azurewebsites.net/api/Preview"



# ---------------------------------------------------
# GENERAL HELPERS
# ---------------------------------------------------
def local_css(file_name: str):
    try:
        with open(file_name, encoding="utf-8") as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    except FileNotFoundError:
        pass
    except UnicodeDecodeError:
        try:
            with open(file_name, encoding="utf-8-sig") as f:
                st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
        except Exception as ex:
            st.warning(f"Could not load CSS file {file_name}: {ex}")


def api_url(base_url: str, path: str) -> str:
    return f"{normalize_base_url(base_url)}/{path.lstrip('/')}"


def safe_get(d: Dict[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        if name in d and d[name] is not None:
            return d[name]
    return default


def as_bool_int(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "y"}
    return False


# ---------------------------------------------------
# BLOB HELPERS
# ---------------------------------------------------
def upload_file_to_blob(file, filename: str, connection_string: str, container: str) -> Tuple[str, str]:
    if not connection_string:
        raise ValueError("Azure Storage connection string is empty. Set AZURE_STORAGE_CONNECTION_STRING.")

    blob_service = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service.get_blob_client(container=container, blob=filename)

    blob_client.upload_blob(file, overwrite=True, max_concurrency=4)

    account_key = getattr(blob_service.credential, "account_key", None)
    if not account_key:
        raise ValueError("Cannot generate SAS URL because account key is unavailable in the storage credential.")

    sas_token = generate_blob_sas(
        account_name=blob_service.account_name,
        container_name=container,
        blob_name=filename,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.now(UTC) + timedelta(hours=3),
    )

    return f"{blob_client.url}?{sas_token}", filename


def delete_blob(blob_name: str, connection_string: str, container: str) -> None:
    if not blob_name or not connection_string:
        return
    try:
        blob_service = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service.get_blob_client(container=container, blob=blob_name)
        blob_client.delete_blob()
        st.info(f"Temporary blob deleted: {blob_name}")
    except Exception as ex:
        st.warning(f"Failed to delete temporary blob {blob_name}: {ex}")


# ---------------------------------------------------
# API HELPERS
# ---------------------------------------------------
def post_json(url: str, payload: Dict[str, Any], verify_ssl: bool, timeout_seconds: int) -> Dict[str, Any]:
    response = requests.post(
        url,
        json=payload,
        headers={"Content-Type": "application/json"},
        verify=verify_ssl,
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    return response.json()


def get_json(url: str, params: Dict[str, Any], verify_ssl: bool, timeout_seconds: int) -> Any:
    clean_params = {k: v for k, v in params.items() if v not in (None, "")}
    response = requests.get(url, params=clean_params, verify=verify_ssl, timeout=timeout_seconds)
    response.raise_for_status()
    return response.json()


def call_preview_api(
    base_url: str,
    preview_path: str,
    blob_url: str,
    firm_id: Optional[int],
    engagement_id: Optional[int],
    country: str,
    enable_llm: bool,
    enable_embedding_search: bool,
    prefix_w: float,
    balance_w: float,
    token_w: float,
    substring_w: float,
    historical_w: float,
    embedding_group_w: float,
    embedding_history_w: float,
    pre_llm_threshold: float,
    needs_review_threshold: float,
    merge_alpha: float,
    top_k: int,
    batch_size: int,
    enable_llm_deduplication: bool,
    verify_ssl: bool,
    timeout_seconds: int,
) -> Dict[str, Any]:
    """Start /api/Preview2. The backend now returns 202 + durable instance/run id."""

    payload = {
        "firmId": firm_id,
        "engagementId": engagement_id,
        "country": country or None,
        "blobUrl": blob_url,
        "enableLlm": enable_llm,
        "enableEmbeddingSearch": enable_embedding_search,
        "weights": {
            "prefix": prefix_w,
            "balance": balance_w,
            "token": token_w,
            "substring": substring_w,
            "historical": historical_w,
            "embeddingGroup": embedding_group_w,
            "embeddingHistory": embedding_history_w,
        },
        "preLlmThreshold": pre_llm_threshold,
        "needsReviewThreshold": needs_review_threshold,
        "mergeAlpha": merge_alpha,
        "topK": top_k,
        "batchSize": batch_size,
        "enableLlmDeduplication": enable_llm_deduplication,
    }

    return post_json(api_url(base_url, preview_path), payload, verify_ssl, timeout_seconds)


def get_durable_status(
    base_url: str,
    status_url: Optional[str],
    instance_id: str,
    verify_ssl: bool,
    timeout_seconds: int,
) -> Dict[str, Any]:
    url = status_url or api_url(base_url, f"/api/Preview2/status/{instance_id}")
    return get_json(url, {}, verify_ssl, timeout_seconds)


def durable_runtime_status(data: Dict[str, Any]) -> str:
    return str(safe_get(data, "runtimeStatus", "RuntimeStatus", "status", "Status", default="")).strip()


def extract_durable_output(data: Dict[str, Any]) -> Dict[str, Any]:
    output = safe_get(data, "serializedOutput", "SerializedOutput", "output", "Output", default=None)
    if isinstance(output, dict):
        return output
    if isinstance(output, str) and output.strip():
        try:
            parsed = json.loads(output)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def wait_for_durable_completion(
    base_url: str,
    start_response: Dict[str, Any],
    verify_ssl: bool,
    timeout_seconds: int,
    poll_interval_seconds: int = 5,
) -> Dict[str, Any]:
    instance_id = safe_get(start_response, "instanceId", "InstanceId", "runId", "RunId")
    if not instance_id:
        return start_response

    status_url = safe_get(start_response, "statusQueryGetUri", "StatusQueryGetUri")
    terminal = {"Completed", "Failed", "Canceled", "Cancelled", "Terminated"}
    deadline = time.monotonic() + max(30, timeout_seconds)
    status_box = st.empty()
    progress = st.progress(0)
    last_status: Dict[str, Any] = {}
    tick = 0

    while time.monotonic() < deadline:
        tick += 1
        last_status = get_durable_status(base_url, status_url, str(instance_id), verify_ssl, timeout_seconds)
        runtime_status = durable_runtime_status(last_status)
        status_box.info(f"Durable run {instance_id}: {runtime_status or 'Running'}")
        progress.progress(min(0.98, tick / 120))

        if runtime_status in terminal:
            progress.progress(1.0)
            return last_status

        time.sleep(max(1, poll_interval_seconds))

    raise TimeoutError(f"Timed out waiting for durable run {instance_id}. You can load results later by RunId.")


def get_suggestions(
    base_url: str,
    run_id: Optional[str],
    firm_id: Optional[int],
    engagement_id: Optional[int],
    status: Optional[str],
    verify_ssl: bool,
    timeout_seconds: int,
) -> Any:
    return get_json(
        api_url(base_url, "/api/coa-auto-mapping/suggestions"),
        {
            "runId": run_id,
            "firmId": firm_id,
            "engagementId": engagement_id,
            "status": status if status and status != "All" else None,
        },
        verify_ssl,
        timeout_seconds,
    )


def submit_feedback(
    base_url: str,
    suggestion_id: str,
    action: str,
    final_group_key: Optional[str],
    user_comment: str,
    submitted_by: str,
    verify_ssl: bool,
    timeout_seconds: int,
) -> Dict[str, Any]:
    payload = {
        "suggestionId": suggestion_id,
        "action": action,
        "finalGroupKey": final_group_key,
        "userComment": user_comment,
        "submittedBy": submitted_by,
    }
    return post_json(api_url(base_url, "/api/coa-auto-mapping/feedback"), payload, verify_ssl, timeout_seconds)


def get_feedback_history(
    base_url: str,
    firm_id: Optional[int],
    engagement_id: Optional[int],
    verify_ssl: bool,
    timeout_seconds: int,
) -> Any:
    return get_json(
        api_url(base_url, "/api/coa-auto-mapping/feedback"),
        {"firmId": firm_id, "engagementId": engagement_id},
        verify_ssl,
        timeout_seconds,
    )


# ---------------------------------------------------
# DISPLAY HELPERS
# ---------------------------------------------------
def extract_rows(resp: Any) -> List[Dict[str, Any]]:
    if isinstance(resp, list):
        return resp
    if not isinstance(resp, dict):
        return []
    return safe_get(resp, "rows", "Rows", "suggestions", "Suggestions", default=[])


def _pick_best_candidate(candidates: List[Any]) -> Optional[Dict[str, Any]]:
    """Return the highest-ranked deterministic candidate for a suggestion.

    Prefers the one with rankNumber == 1; otherwise the one with the highest
    candidateScore. Returns None when there are no candidates.
    """
    if not candidates:
        return None
    best: Optional[Dict[str, Any]] = None
    best_score = float("-inf")
    for c in candidates:
        if not isinstance(c, dict):
            continue
        rank = safe_get(c, "rankNumber", "RankNumber")
        if rank == 1 or rank == "1":
            return c
        raw_score = safe_get(c, "candidateScore", "CandidateScore")
        try:
            score_val = float(raw_score) if raw_score is not None else float("-inf")
        except (TypeError, ValueError):
            score_val = float("-inf")
        if score_val > best_score:
            best_score = score_val
            best = c
    return best


def flatten_suggestions(data: Any) -> pd.DataFrame:
    rows = extract_rows(data)
    flat_rows: List[Dict[str, Any]] = []

    for r in rows:
        if not isinstance(r, dict):
            continue
        signals = safe_get(r, "signals", "Signals", default={}) or {}
        candidates = safe_get(r, "candidates", "Candidates", default=[]) or []

        best_candidate = _pick_best_candidate(candidates)
        best_candidate_label = ""
        best_candidate_score = None
        if best_candidate:
            grp = safe_get(best_candidate, "groupName", "GroupName", "group", "Group") or ""
            cls = safe_get(best_candidate, "classification", "Classification") or ""
            label_parts = [str(p) for p in (cls, grp) if p]
            best_candidate_label = " / ".join(label_parts) if label_parts else (
                safe_get(best_candidate, "groupKey", "GroupKey") or ""
            )
            best_candidate_score = safe_get(best_candidate, "candidateScore", "CandidateScore")

        flat_rows.append(
            {
                "RunId": safe_get(r, "runId", "RunId"),
                "SuggestionId": safe_get(r, "suggestionId", "SuggestionId"),
                "Status": safe_get(r, "status", "Status"),
                "AccountNumber": safe_get(r, "accountNumber", "AccountNumber"),
                "AccountName": safe_get(r, "accountName", "AccountName"),
                "Description": safe_get(r, "description", "Description"),
                "Amount": safe_get(r, "amount", "Amount"),
                "BalanceType": safe_get(r, "balanceType", "BalanceType"),
                # The persisted suggestion record (AutoMappingSuggestionRecord) does NOT
                # carry a textual LLM column - only the LlmWasCalled / LlmSkippedByCache
                # booleans. Derive the same "Used / SkippedByCache / Not Used" label that
                # the in-memory Preview2Row exposes so the saved-suggestions table is not
                # always blank for this column.
                "LLM": (
                    safe_get(r, "llm", "LLM")
                    or (
                        "Used" if safe_get(r, "llmWasCalled", "LlmWasCalled", default=False)
                        else "SkippedByCache" if safe_get(r, "llmSkippedByCache", "LlmSkippedByCache", default=False)
                        else "Not Used"
                    )
                ),
                "LlmWasCalled": as_bool_int(safe_get(r, "llmWasCalled", "LlmWasCalled", default=False)),
                "LlmSkippedByCache": as_bool_int(safe_get(r, "llmSkippedByCache", "LlmSkippedByCache", default=False)),
                "SuggestedGroupKey": safe_get(r, "suggestedGroupKey", "SuggestedGroupKey"),
                "Type": safe_get(r, "type", "Type", "suggestedType", "SuggestedType"),
                "Classification": safe_get(r, "classification", "Classification", "suggestedClassification", "SuggestedClassification"),
                "Group": safe_get(r, "group", "Group", "suggestedGroup", "SuggestedGroup"),
                "NumberSeed": safe_get(r, "numberSeed", "NumberSeed", "suggestedNumberSeed", "SuggestedNumberSeed"),
                "Country": safe_get(r, "country", "Country", "suggestedCountry", "SuggestedCountry"),
                "ConfidenceScore": safe_get(r, "confidenceScore", "ConfidenceScore", "finalScore", "FinalScore"),
                "ConfidenceBand": safe_get(r, "confidenceBand", "ConfidenceBand"),
                "ReviewRequired": as_bool_int(safe_get(r, "reviewRequired", "ReviewRequired", default=False)),
                "MappingRationale": safe_get(r, "mappingRationale", "MappingRationale", "reason", "Reason"),
                "PrefixMatchScore": safe_get(signals, "prefixMatchScore", "PrefixMatchScore") or safe_get(r, "prefixMatchScore", "PrefixMatchScore"),
                "BalanceDirectionScore": safe_get(signals, "balanceDirectionScore", "BalanceDirectionScore") or safe_get(r, "balanceDirectionScore", "BalanceDirectionScore"),
                "TokenOverlapScore": safe_get(signals, "tokenOverlapScore", "TokenOverlapScore") or safe_get(r, "tokenOverlapScore", "TokenOverlapScore"),
                "SubstringScore": safe_get(signals, "substringScore", "SubstringScore") or safe_get(r, "substringScore", "SubstringScore"),
                "HistoricalFeedbackScore": safe_get(signals, "historicalFeedbackScore", "HistoricalFeedbackScore") or safe_get(r, "historicalFeedbackScore", "HistoricalFeedbackScore"),
                "DeterministicBestScore": safe_get(signals, "preLlmConfidenceScore", "PreLlmConfidenceScore") or safe_get(r, "preLlmConfidenceScore", "PreLlmConfidenceScore"),
                "LlmConfidence": safe_get(r, "llmScore", "LlmScore"),
                "FinalConfidence": safe_get(r, "confidenceScore", "ConfidenceScore", "finalScore", "FinalScore"),
                "BestCandidate": best_candidate_label,
                "BestCandidateScore": best_candidate_score,
                "CandidatesCount": len(candidates),
            }
        )

    return pd.DataFrame(flat_rows)


def show_run_summary(resp: Dict[str, Any]) -> None:
    run_id = safe_get(resp, "runId", "RunId")
    if run_id:
        st.session_state["last_run_id"] = run_id

    summary_fields = {
        "RunId": run_id,
        "Status": safe_get(resp, "status", "Status"),
        "Total": safe_get(resp, "totalAccounts", "TotalAccounts"),
        "Processed": safe_get(resp, "processedCount", "ProcessedCount"),
        "High Confidence": safe_get(resp, "highConfidenceCount", "HighConfidenceCount"),
        "Needs Review": safe_get(resp, "needsReviewCount", "NeedsReviewCount"),
        "Manual Required": safe_get(resp, "manualRequiredCount", "ManualRequiredCount"),
        "LLM Eligible": safe_get(resp, "llmEligibleCount", "LlmEligibleCount"),
        "LLM Called": safe_get(resp, "llmCalledCount", "LlmCalledCount"),
        "LLM Cache Skipped": safe_get(resp, "llmSkippedByCacheCount", "LlmSkippedByCacheCount"),
        "Failed": safe_get(resp, "failedCount", "FailedCount"),
    }

    st.subheader("Run summary")
    metric_cols = st.columns(5)
    metric_cols[0].metric("Processed", summary_fields["Processed"] or 0)
    metric_cols[1].metric("High", summary_fields["High Confidence"] or 0)
    metric_cols[2].metric("Needs Review", summary_fields["Needs Review"] or 0)
    metric_cols[3].metric("Manual", summary_fields["Manual Required"] or 0)
    metric_cols[4].metric("LLM Called", summary_fields["LLM Called"] or 0)

    with st.expander("Raw run summary", expanded=False):
        st.json(summary_fields)



def show_suggestions_table(data: Any, title: str = "Suggestions", key_suffix: str = "") -> pd.DataFrame:
    df = flatten_suggestions(data)
    st.subheader(title)
    if df.empty:
        st.warning("No suggestions found.")
        return df

    st.dataframe(df, use_container_width=True, height=500)

    csv = df.to_csv(index=False).encode("utf-8")
    # Streamlit auto-generates widget ids from (type + parameters). When this helper
    # is rendered on more than one tab in the same run (e.g. Run tab + Review tab)
    # the two download buttons collide. Derive a stable, unique key from the title.
    button_key = "download_suggestions_csv__" + "".join(
        ch if ch.isalnum() else "_" for ch in title.lower()
    )
    if key_suffix:
        button_key = f"{button_key}__{key_suffix}"
    st.download_button(
        "Download suggestions CSV",
        data=csv,
        file_name="automap_suggestions.csv",
        mime="text/csv",
        use_container_width=True,
        key=button_key,
    )
    return df


# ---------------------------------------------------
# UI TABS
# ---------------------------------------------------
def render_sidebar() -> Dict[str, Any]:
    st.sidebar.header("API / Storage config")

    base_url = st.sidebar.text_input("API base URL", value=DEFAULT_API_BASE_URL or "")
    preview_path = st.sidebar.text_input("Preview endpoint", value=DEFAULT_PREVIEW_PATH or "/api/Preview2")
    container = st.sidebar.text_input("Blob container", value=DEFAULT_CONTAINER or "testcontainer")

    use_ssl_verify = st.sidebar.checkbox("Verify SSL", value=True)
    timeout_seconds = st.sidebar.number_input(
        "Durable wait timeout seconds",
        min_value=30,
        max_value=7200,
        value=1800,
        step=30,
        help="Used for short API calls and as the maximum time Streamlit waits while polling the durable run.",
    )

    with st.sidebar.expander("Storage connection", expanded=False):
        storage_connection_string = st.text_input(
            "AZURE_STORAGE_CONNECTION_STRING",
            value=DEFAULT_STORAGE_CONNECTION_STRING or "",
            type="password",
            help="Do not hardcode secrets in app.py. Use Streamlit secrets or environment variables.",
        )

    return {
        "base_url": base_url,
        "preview_path": preview_path,
        "container": container,
        "storage_connection_string": storage_connection_string,
        "verify_ssl": use_ssl_verify,
        "timeout_seconds": int(timeout_seconds),
    }


def render_run_tab(config: Dict[str, Any]) -> None:
    st.subheader("1. Upload Trial Balance / COA file")
    st.caption("Excel columns expected by API: accountNumber, accountName, description, amount.")

    uploaded_file = st.file_uploader("Choose file (.xlsx/.xls)", type=["xls", "xlsx"], key="tb_file")

    st.subheader("2. Run settings")
    c1, c2, c3 = st.columns(3)

    with c1:
        firm_id_raw = st.text_input("FirmId", value="123")
        engagement_id_raw = st.text_input("EngagementId", value="456")
        country = st.text_input("Country", value="US")

    with c2:
        enable_llm = st.checkbox("Enable LLM fallback", value=True)
        enable_embedding_search = st.checkbox("Enable embedding search", value=False)
        enable_llm_deduplication = st.checkbox("Enable LLM deduplication/cache", value=True)

    with c3:
        batch_size = st.number_input("Batch size", min_value=10, max_value=1000, value=250, step=10)

    st.subheader("3. Scoring parameters")
    w1, w2, w3, w4 = st.columns(4)
    with w1:
        prefix_w = st.number_input("Prefix weight", 0.0, 1.0, 0.20, 0.01)
        balance_w = st.number_input("Balance weight", 0.0, 1.0, 0.20, 0.01)
    with w2:
        token_w = st.number_input("Token weight", 0.0, 1.0, 0.2, 0.01)
        substring_w = st.number_input("Substring weight", 0.0, 1.0, 0.2, 0.01)
    with w3:
        historical_w = st.number_input("Historical weight", 0.0, 1.0, 0.20, 0.01)
        embedding_group_w = st.number_input("Embedding group weight", 0.0, 1.0, 0.10, 0.01)
    with w4:
        embedding_history_w = st.number_input("Embedding history weight", 0.0, 1.0, 0.15, 0.01)
        top_k = st.number_input("TopK candidates", min_value=1, max_value=590, value=25, step=1)

    t1, t2, t3 = st.columns(3)
    with t1:
        pre_llm_threshold = st.number_input("Pre-LLM threshold", 0.0, 1.0, 0.74, 0.01, format="%.2f")
    with t2:
        needs_review_threshold = st.number_input("Needs-review threshold", 0.0, 1.0, 0.75, 0.01, format="%.2f")
    with t3:
        merge_alpha_choice = st.selectbox(
            "Merge alpha",
            options=[
                "Balanced - 0.60 (recommended)",
                "Conservative - 0.70 (trust deterministic more)",
                "LLM-assisted - 0.50 (trust LLM more)",
            ],
            index=0,
            help=(
                "Controls final confidence after LLM: "
                "final = alpha * deterministicBestScore + (1-alpha) * llmConfidence. "
                "Recommended default is 0.60 for financial mapping: deterministic scoring remains primary, "
                "while LLM can still improve borderline matches."
            ),
        )
        merge_alpha = {
            "Balanced - 0.60 (recommended)": 0.60,
            "Conservative - 0.70 (trust deterministic more)": 0.70,
            "LLM-assisted - 0.50 (trust LLM more)": 0.50,
        }[merge_alpha_choice]

    weight_sum = prefix_w + balance_w + token_w + substring_w + historical_w
    if enable_embedding_search:
        weight_sum += embedding_group_w + embedding_history_w
    st.caption(f"Current effective weight sum: {weight_sum:.2f}")

    run_clicked = st.button("Run AutoMap", use_container_width=True, type="primary")

    if not run_clicked:
        return

    if uploaded_file is None:
        st.error("Please upload a file first.")
        return

    try:
        firm_id = int(firm_id_raw) if firm_id_raw.strip() else None
        engagement_id = int(engagement_id_raw) if engagement_id_raw.strip() else None
    except ValueError:
        st.error("FirmId and EngagementId must be numeric.")
        return

    unique_file_name = f"ca-{uuid.uuid4()}-{uploaded_file.name}"
    blob_name = ""
    durable_finished = False

    try:
        with st.spinner("Uploading file to Azure Blob Storage..."):
            blob_url, blob_name = upload_file_to_blob(
                uploaded_file,
                unique_file_name,
                config["storage_connection_string"],
                config["container"],
            )

        with st.expander("Blob SAS URL", expanded=False):
            st.code(blob_url, language="text")

        with st.spinner("Starting durable AutoMap run..."):
            start_result = call_preview_api(
                base_url=config["base_url"],
                preview_path=config["preview_path"],
                blob_url=blob_url,
                firm_id=firm_id,
                engagement_id=engagement_id,
                country=country,
                enable_llm=enable_llm,
                enable_embedding_search=enable_embedding_search,
                prefix_w=prefix_w,
                balance_w=balance_w,
                token_w=token_w,
                substring_w=substring_w,
                historical_w=historical_w,
                embedding_group_w=embedding_group_w,
                embedding_history_w=embedding_history_w,
                pre_llm_threshold=pre_llm_threshold,
                needs_review_threshold=needs_review_threshold,
                merge_alpha=merge_alpha,
                top_k=int(top_k),
                batch_size=int(batch_size),
                enable_llm_deduplication=enable_llm_deduplication,
                verify_ssl=config["verify_ssl"],
                timeout_seconds=config["timeout_seconds"],
            )

        run_id = safe_get(start_result, "runId", "RunId", "instanceId", "InstanceId")
        if run_id:
            st.session_state["last_run_id"] = run_id
        st.success(f"Durable AutoMap run started: {run_id}")
        with st.expander("Durable start response", expanded=False):
            st.json(start_result)

        with st.spinner("Waiting for durable AutoMap run to complete..."):
            status_result = wait_for_durable_completion(
                config["base_url"],
                start_result,
                config["verify_ssl"],
                config["timeout_seconds"],
            )

        runtime_status = durable_runtime_status(status_result)
        durable_output = extract_durable_output(status_result)
        durable_finished = runtime_status in {"Completed", "Failed", "Canceled", "Cancelled", "Terminated"}
        if runtime_status == "Completed":
            st.success("AutoMap run completed.")
        else:
            st.warning(f"Durable run finished with status: {runtime_status}")

        summary_result = durable_output or status_result
        show_run_summary(summary_result)

        run_id = safe_get(summary_result, "runId", "RunId", default=run_id)
        if run_id:
            suggestions = get_suggestions(
                config["base_url"],
                run_id,
                firm_id,
                engagement_id,
                "All",
                config["verify_ssl"],
                config["timeout_seconds"],
            )
            st.session_state["suggestions_data"] = suggestions
            show_suggestions_table(suggestions, "Saved suggestions", key_suffix="run")
        else:
            st.info("No RunId was returned. Check the durable status response.")

    except Exception as ex:
        st.error(f"AutoMap run failed: {ex}")
    finally:
        if blob_name and durable_finished:
            delete_blob(blob_name, config["storage_connection_string"], config["container"])
        elif blob_name:
            st.info("Temporary blob was kept because the durable run may still be using it.")


def render_review_tab(config: Dict[str, Any]) -> None:
    st.subheader("Review saved suggestions")

    last_run_id = st.session_state.get("last_run_id", "")
    f1, f2, f3, f4 = st.columns(4)
    with f1:
        run_id = st.text_input("RunId", value=last_run_id)
    with f2:
        firm_id_raw = st.text_input("FirmId filter", value="")
    with f3:
        engagement_id_raw = st.text_input("EngagementId filter", value="")
    with f4:
        status = st.selectbox("Status", ["All", "HighConfidence", "NeedsReview", "ManualRequired", "Approved", "Corrected", "Rejected", "Failed"])

    load_clicked = st.button("Load suggestions", use_container_width=True)

    if load_clicked:
        try:
            firm_id = int(firm_id_raw) if firm_id_raw.strip() else None
            engagement_id = int(engagement_id_raw) if engagement_id_raw.strip() else None
            data = get_suggestions(
                config["base_url"],
                run_id or None,
                firm_id,
                engagement_id,
                status,
                config["verify_ssl"],
                config["timeout_seconds"],
            )
            st.session_state["suggestions_data"] = data
        except Exception as ex:
            st.error(f"Failed to load suggestions: {ex}")

    data = st.session_state.get("suggestions_data")
    df = show_suggestions_table(data, "Saved suggestions", key_suffix="review") if data is not None else pd.DataFrame()

    st.markdown("---")
    st.subheader("Submit feedback")

    if df.empty:
        st.info("Load suggestions first, then select a SuggestionId for feedback.")
        return

    suggestion_options = [x for x in df["SuggestionId"].dropna().astype(str).tolist() if x]
    selected_suggestion_id = st.selectbox("SuggestionId", suggestion_options)

    selected_row = df[df["SuggestionId"].astype(str) == selected_suggestion_id].head(1)
    suggested_group_key = ""
    if not selected_row.empty:
        suggested_group_key = str(selected_row.iloc[0].get("SuggestedGroupKey") or "")
        st.caption(f"Suggested group key: {suggested_group_key}")

    action = st.selectbox("Action", ["Approved", "Corrected", "Rejected"])
    final_group_key = st.text_input(
        "FinalGroupKey",
        value=suggested_group_key if action == "Approved" else "",
        help="For Approved, use suggested group key. For Corrected, enter selected final group key. For Rejected, leave empty.",
    )
    submitted_by = st.text_input("Submitted by", value="user@email.com")
    user_comment = st.text_area("Comment", value="")

    if st.button("Submit feedback", use_container_width=True, type="primary"):
        try:
            result = submit_feedback(
                config["base_url"],
                selected_suggestion_id,
                action,
                final_group_key or None,
                user_comment,
                submitted_by,
                config["verify_ssl"],
                config["timeout_seconds"],
            )
            st.success("Feedback saved.")
            st.json(result)
        except Exception as ex:
            st.error(f"Failed to submit feedback: {ex}")


def render_feedback_tab(config: Dict[str, Any]) -> None:
    st.subheader("Feedback history")

    c1, c2 = st.columns(2)
    with c1:
        firm_id_raw = st.text_input("FirmId", value="123", key="fb_firm")
    with c2:
        engagement_id_raw = st.text_input("EngagementId", value="456", key="fb_eng")

    if st.button("Load feedback history", use_container_width=True):
        try:
            firm_id = int(firm_id_raw) if firm_id_raw.strip() else None
            engagement_id = int(engagement_id_raw) if engagement_id_raw.strip() else None
            data = get_feedback_history(
                config["base_url"],
                firm_id,
                engagement_id,
                config["verify_ssl"],
                config["timeout_seconds"],
            )
            if isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                df = pd.DataFrame(extract_rows(data))
            if df.empty:
                st.warning("No feedback found.")
            else:
                st.dataframe(df, use_container_width=True, height=500)
                st.download_button(
                    "Download feedback CSV",
                    data=df.to_csv(index=False).encode("utf-8"),
                    file_name="automap_feedback.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
        except Exception as ex:
            st.error(f"Failed to load feedback: {ex}")


def render_help_tab() -> None:
    st.subheader("Durable 10K account processing flow")
    st.markdown(
        """
1. Upload Excel file to Blob Storage.
2. Streamlit calls `/api/Preview2` with `blobUrl`, `batchSize`, and scoring settings.
3. `/api/Preview2` starts a Durable orchestration and immediately returns `instanceId` / `runId`.
4. Streamlit polls `/api/Preview2/status/{instanceId}`.
5. The Durable activity loads `group.json`, reads the blob, embeds accounts, and runs deterministic + LLM scoring.
6. Suggestions and candidates are saved to local SQLite by `RunId`.
7. When the durable run completes, Streamlit loads saved suggestions by `RunId`.
8. Use the Review tab to approve/correct/reject.
9. Saved feedback improves `HistoricalFeedbackScore` in future runs.
        """
    )

    st.subheader("Required backend endpoints")
    st.code(
        """POST /api/Preview2
GET  /api/Preview2/status/{instanceId}
GET  /api/coa-auto-mapping/suggestions?runId={runId}
POST /api/coa-auto-mapping/feedback
GET  /api/coa-auto-mapping/feedback?firmId={firmId}&engagementId={engagementId}""",
        language="text",
    )

    st.warning(
        "Do not hardcode Azure Storage keys in app.py. Use Streamlit secrets or environment variables. "
        "If a real key was committed or shared, rotate it."
    )


# ---------------------------------------------------
# MAIN
# ---------------------------------------------------
def main() -> None:
    st.set_page_config(page_title="AutoMap Testing", layout="wide")
    local_css("style.css")

    st.markdown(
        "<img src='https://cdn.wolterskluwer.io/wk/fundamentals/1.15.2/logo/assets/medium.svg' "
        "alt='Wolters Kluwer Logo' width='190px' height='31px'>",
        unsafe_allow_html=True,
    )
    st.title(f"AutoMap v{VERSION}")

    config = render_sidebar()

    tab_run, tab_review, tab_feedback, tab_help = st.tabs(
        ["Run AutoMap", "Review Suggestions", "Feedback History", "Help / Flow"]
    )

    with tab_run:
        render_run_tab(config)
    with tab_review:
        render_review_tab(config)
    with tab_feedback:
        render_feedback_tab(config)
    with tab_help:
        render_help_tab()


if __name__ == "__main__":
    main()
