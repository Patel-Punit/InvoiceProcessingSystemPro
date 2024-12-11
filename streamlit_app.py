import streamlit as st
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from accuracy_check import validate_invoice_data

def fetch_and_create_dataframes(api_url, payload, uploaded_file):
    try:
        files = {"file": ("invoice.pdf", uploaded_file)}
        response = requests.post(api_url, data=payload, files=files)

        if response.status_code == 200:
            response_json = response.json()
            
            # Extracting DataFrames
            invoice_details = response_json.get("Invoice Details", {})
            invoice_df = pd.DataFrame([invoice_details])

            line_items = response_json.get("Line Items", [])
            line_items_df = pd.DataFrame(line_items)

            total_summary = response_json.get("Total Summary", {})
            total_summary_df = pd.DataFrame([total_summary])

            return invoice_df, line_items_df, total_summary_df, response_json
        else:
            st.error(f"Failed to fetch data. Status code: {response.status_code}, Error: {response.text}")
            return None, None, None, None
    except Exception as e:
        st.error(f"An error occurred while fetching data: {e}")
        return None, None, None, None

# Set page config
st.set_page_config(page_title="Invoice Processor", layout="wide")

# Title
st.title("Invoice Processing System")

# API Configuration
api_url = "https://pluto.origamis.ai:9001/zolvit/docagent_zolvit"
payload = {
    "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6InYtYXZpbmFzaC5rQG9yaWdhbWlzLmFpIiwicm9sZSI6IkFkbWluIiw",
    "email": "user@zolvit.com",
}

# File uploader
uploaded_file = st.file_uploader("Choose a PDF file", type=['pdf'])

if uploaded_file is not None:
    # Process button
    if st.button("Process Invoice"):
        with st.spinner('Processing invoice...'):
            # Fetch data and create dataframes
            invoice_df, line_items_df, total_summary_df, raw_response = fetch_and_create_dataframes(
                api_url, payload, uploaded_file
            )

            if invoice_df is not None:
                # Create tabs for different views
                tab1, tab2, tab3, tab4, tab5 = st.tabs([
                    "Raw Response", 
                    "Invoice Details", 
                    "Line Items", 
                    "Total Summary",
                    "Validation Results"
                ])

                with tab1:
                    st.json(raw_response)

                with tab2:
                    st.subheader("Invoice Details")
                    st.dataframe(invoice_df)

                with tab3:
                    st.subheader("Line Items")
                    st.dataframe(line_items_df)

                with tab4:
                    st.subheader("Total Summary")
                    st.dataframe(total_summary_df)

                with tab5:
                    st.subheader("Validation Results")
                    validation_results = validate_invoice_data(invoice_df, line_items_df, total_summary_df)
                    st.write(validation_results)

else:
    st.info("Please upload a PDF invoice to begin processing.")
