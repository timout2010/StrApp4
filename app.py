import streamlit as st
import pandas as pd

# Sample data
data = {
    "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25],
    "accountTypeAbbrev": ["A", "A", "A", "A", "A", "A", "A", "A", "L", "L", "L", "L", "L", "L", "L", "E", "R", "X", "R", "R", "X", "X", "X", "X", "X"],
    "accountType": [
        "Asset", "Asset", "Asset", "Asset", "Asset", "Asset", "Asset", "Asset",
        "Liability", "Liability", "Liability", "Liability", "Liability", "Liability", "Liability",
        "Equity", "Revenue", "Expense", "Revenue", "Revenue", "Expense", "Expense", "Expense", "Expense", "Expense"
    ],
    "accountSubTypeAbbrev": [
        "CA", "CA", "CA", "CA", "CA", "NA", "NA", "NA", "CL", "CL", "CL", "CL",
        "NL", "NL", "NL", "EQ", "REV", "COR", "OI", "OI", "OPX", "OPX", "OE", "OE", "OE"
    ],
    "AccountSubType": [
        "Current Assets", "Current Assets", "Current Assets", "Current Assets", "Current Assets",
        "Non-current Assets", "Non-current Assets", "Non-current Assets",
        "Current Liabilities", "Current Liabilities", "Current Liabilities", "Current Liabilities",
        "Non-current Liabilities", "Non-current Liabilities", "Non-current Liabilities",
        "Equity", "Revenues", "Costs of Revenue", "Other Income", "Other Income",
        "Operating Expenses", "Operating Expenses", "Other Expenses", "Other Expenses", "Other Expenses"
    ],
    "fsCaptionIndex": [
        1000, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 2000, 2100, 2200, 2300,
        2400, 2500, 2600, 3000, 4000, 4100, 4200, 4300, 5000, 5500, 6000, 6500, 7000
    ],
    "fsCaption": [
        "Cash and Equivalents", "Receivables, net", "Inventory", "Prepaid Expenses", "Other Current Assets",
        "Property, Plant and Equipment", "Intangibles", "Other Assets", "Payables", "Accrued Expenses",
        "Deferred Revenue", "Other Current Liabilities", "Long-Term Debt", "Deferred Taxes", "Other Liabilities",
        "Equity", "Revenue", "Cost of Revenue", "Interest Income", "Other Income", 
        "Selling, General and Administrative", "Depreciation & Amortization", "Other Expense", "Interest Expense", "Income Tax Expense"
    ]
}

# Create DataFrame
account_df = pd.DataFrame(data)

# Streamlit app
st.title("Account Data Viewer")

# Display the DataFrame
st.subheader("Complete Account Data")
st.dataframe(account_df)

# Add filters
st.sidebar.header("Filters")
account_type = st.sidebar.multiselect("Filter by Account Type", options=account_df["accountType"].unique(), default=None)
subtype = st.sidebar.multiselect("Filter by Subtype", options=account_df["AccountSubType"].unique(), default=None)

# Apply filters
filtered_df = account_df
if account_type:
    filtered_df = filtered_df[filtered_df["accountType"].isin(account_type)]
if subtype:
    filtered_df = filtered_df[filtered_df["AccountSubType"].isin(subtype)]

# Display filtered results
st.subheader("Filtered Account Data")
st.dataframe(filtered_df)

# Download option
st.download_button(
    label="Download Filtered Data as CSV",
    data=filtered_df.to_csv(index=False),
    file_name="filtered_account_data.csv",
    mime="text/csv",
)

