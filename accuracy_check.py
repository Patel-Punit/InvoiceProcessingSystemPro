import pandas as pd
import numpy as np
from datetime import datetime

def validate_invoice_data(invoice_df, line_items_df, total_summary_df):
    """
    Validates the accuracy of invoice data across three dataframes by checking for missing values,
    data types, and relational integrity.
    
    Parameters:
    -----------
    invoice_df : pd.DataFrame
        DataFrame containing invoice header information
    line_items_df : pd.DataFrame
        DataFrame containing line item details
    total_summary_df : pd.DataFrame
        DataFrame containing invoice summary totals
    
    Returns:
    --------
    tuple : (bool, str, str)
        (passed, failed_step, failure_details)
    """
    
    # Convert all None, blank, empty, etc to NaN for consistency
    def standardize_nulls(df):
        # Replace empty strings and string 'nan'/'null' with NaN
        return df.replace(['', 'nan', 'null', 'none', 'None'], np.nan)
    
    invoice_df = standardize_nulls(invoice_df)
    line_items_df = standardize_nulls(line_items_df)
    total_summary_df = standardize_nulls(total_summary_df)
    
    # Step 1: Check missing values
    def check_missing_values():
        # Check invoice_df
        invoice_condition = (
            invoice_df['invoice_number'].notna() &
            invoice_df['invoice_date'].notna() &
            invoice_df['place_of_supply'].notna() &
            invoice_df['place_of_origin'].notna() &
            invoice_df['receiver_name'].notna() &
            invoice_df['gstin_supplier'].notna() &
            (invoice_df['taxable_value'].notna() | invoice_df['invoice_value'].notna()) &
            invoice_df['tax_amount'].notna()
        )
        
        if not invoice_condition.all():
            missing_cols = []
            row_idx = invoice_condition[~invoice_condition].index[0]
            for col in invoice_df.columns:
                if invoice_df[col].isna().iloc[row_idx]:
                    missing_cols.append(col)
            return False, f"Missing required values in invoice_df: {', '.join(missing_cols)} at row {row_idx}"
            
        # Check line_items_df
        line_items_condition = (
            ((line_items_df['tax_amount'].notna()) |
             (line_items_df['tax_rate'].notna()) |
             (line_items_df['sgst_amount'].notna() & line_items_df['cgst_amount'].notna()) |
             (line_items_df['igst_amount'].notna()) |
             (line_items_df['sgst_rate'].notna() & line_items_df['cgst_rate'].notna()) |
             (line_items_df['igst_rate'].notna())) &
            (line_items_df['final_amount'].notna() |
             line_items_df['taxable_value'].notna() |
             (line_items_df['rate_per_item_after_discount'].notna() & line_items_df['quantity'].notna()))
        )
        
        if not line_items_condition.all():
            row_idx = line_items_condition[~line_items_condition].index[0]
            return False, f"Missing required values in line_items_df at row {row_idx}"
            
        # Check total_summary_df
        summary_condition = (
            (total_summary_df['total_taxable_value'].notna() |
             total_summary_df['total_invoice_value'].notna()) &
            (total_summary_df['total_tax_amount'].notna() |
             total_summary_df['total_igst_amount'].notna() |
             (total_summary_df['total_cgst_amount'].notna() &
              total_summary_df['total_sgst_amount'].notna()))
        )
        
        if not summary_condition.all():
            row_idx = summary_condition[~summary_condition].index[0]
            return False, f"Missing required values in total_summary_df at row {row_idx}"
            
        return True, ""

    # Step 2: Check data types
    def check_data_types():
        try:
            # Check invoice_df date formats
            invoice_df['invoice_date'] = pd.to_datetime(invoice_df['invoice_date'], format='%d-%b-%y')
            
            # Check numeric columns in invoice_df
            numeric_cols_invoice = ['place_of_supply', 'place_of_origin', 'taxable_value', 
                                  'invoice_value', 'tax_amount']
            for col in numeric_cols_invoice:
                if not pd.isna(invoice_df[col]).all():
                    pd.to_numeric(invoice_df[col].dropna())
            
            # Check line_items_df numeric columns
            numeric_cols_items = ['quantity', 'rate_per_item_after_discount', 'taxable_value',
                                'sgst_amount', 'cgst_amount', 'igst_amount', 'sgst_rate',
                                'cgst_rate', 'igst_rate', 'tax_amount', 'tax_rate', 'final_amount']
            
            for col in numeric_cols_items:
                if not pd.isna(line_items_df[col]).all():
                    pd.to_numeric(line_items_df[col].dropna())
            
            # Check total_summary_df numeric columns
            numeric_cols_summary = ['total_taxable_value', 'total_cgst_amount', 'total_sgst_amount',
                                  'total_igst_amount', 'total_tax_amount', 'total_invoice_value',
                                  'rounding_adjustment']
            
            for col in numeric_cols_summary:
                if not pd.isna(total_summary_df[col]).all():
                    pd.to_numeric(total_summary_df[col].dropna())
            
            return True, ""
        except Exception as e:
            return False, f"Data type conversion failed: {str(e)}"

    # Step 3: Check relations
    def check_relations():
        # Convert numeric columns to float for calculations
        numeric_cols = {
            'invoice_df': ['taxable_value', 'invoice_value', 'tax_amount'],
            'line_items_df': ['rate_per_item_after_discount', 'quantity', 'taxable_value',
                            'sgst_amount', 'cgst_amount', 'igst_amount', 'sgst_rate',
                            'cgst_rate', 'igst_rate', 'tax_amount', 'tax_rate', 'final_amount'],
            'total_summary_df': ['total_taxable_value', 'total_cgst_amount', 'total_sgst_amount',
                               'total_igst_amount', 'total_tax_amount', 'total_invoice_value',
                               'rounding_adjustment']
        }
        
        for df_name, cols in numeric_cols.items():
            df = locals()[df_name]
            for col in cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Check invoice_df relations
        for idx, row in invoice_df.iterrows():
            if not pd.isna(row['invoice_value']) and not pd.isna(row['taxable_value']) and not pd.isna(row['tax_amount']):
                if not np.isclose(row['invoice_value'], row['taxable_value'] + row['tax_amount'], rtol=1e-05):
                    return False, f"Invoice value mismatch at row {idx}: {row['invoice_value']} != {row['taxable_value']} + {row['tax_amount']}"
        
        # Check line_items_df relations
        for idx, row in line_items_df.iterrows():
            # Calculate tax variables based on available data
            if not pd.isna(row['taxable_value']):
                base_value = row['taxable_value']
            elif not pd.isna(row['rate_per_item_after_discount']) and not pd.isna(row['quantity']):
                base_value = row['rate_per_item_after_discount'] * row['quantity']
            else:
                continue
            
            # Calculate different tax amounts
            tax_vars = []
            
            # From individual amounts
            if not pd.isna(row['sgst_amount']) and not pd.isna(row['cgst_amount']) and not pd.isna(row['igst_amount']):
                tax_from_individual_amounts = row['sgst_amount'] + row['cgst_amount'] + row['igst_amount']
                tax_vars.append(tax_from_individual_amounts)
            
            # From individual rates
            if not pd.isna(row['sgst_rate']) and not pd.isna(row['cgst_rate']) and not pd.isna(row['igst_rate']):
                tax_from_individual_rates = base_value * (row['sgst_rate'] + row['cgst_rate'] + row['igst_rate']) / 100
                tax_vars.append(tax_from_individual_rates)
            
            # From total tax amount
            if not pd.isna(row['tax_amount']):
                tax_vars.append(row['tax_amount'])
            
            # From total tax rate
            if not pd.isna(row['tax_rate']):
                tax_from_rate = base_value * row['tax_rate'] / 100
                tax_vars.append(tax_from_rate)
            
            # Check if all tax calculations match
            if len(tax_vars) >= 2:
                for i in range(len(tax_vars)-1):
                    if not np.isclose(tax_vars[i], tax_vars[i+1], rtol=1e-05):
                        return False, f"Tax amount mismatch at row {idx}: Different tax calculations yield different results"
            
            # Check final amount
            if not pd.isna(row['final_amount']):
                tax_amount = next((t for t in tax_vars if not pd.isna(t)), None)
                if tax_amount is not None:
                    if not np.isclose(row['final_amount'], base_value + tax_amount, rtol=1e-05):
                        return False, f"Final amount mismatch at row {idx}: {row['final_amount']} != {base_value} + {tax_amount}"
        
        # Check total_summary_df relations
        for idx, row in total_summary_df.iterrows():
            # Calculate total tax
            if pd.isna(row['total_tax_amount']):
                if not pd.isna(row['total_cgst_amount']) and not pd.isna(row['total_sgst_amount']) and not pd.isna(row['total_igst_amount']):
                    total_tax = row['total_cgst_amount'] + row['total_sgst_amount'] + row['total_igst_amount']
            else:
                total_tax = row['total_tax_amount']
            
            # Check invoice total
            if not pd.isna(row['total_taxable_value']) and not pd.isna(row['total_invoice_value']) and not pd.isna(total_tax):
                if not np.isclose(row['total_invoice_value'], row['total_taxable_value'] + total_tax, rtol=1e-05):
                    return False, f"Total invoice value mismatch at row {idx}: {row['total_invoice_value']} != {row['total_taxable_value']} + {total_tax}"
        
        return True, ""

    # Execute validation steps
    # Step 1: Missing values
    passed_step1, error_msg = check_missing_values()
    if not passed_step1:
        return False, "Step 1: Missing Values", error_msg
    
    # Step 2: Data types
    passed_step2, error_msg = check_data_types()
    if not passed_step2:
        return False, "Step 2: Data Types", error_msg
    
    # Step 3: Relations
    passed_step3, error_msg = check_relations()
    if not passed_step3:
        return False, "Step 3: Relations", error_msg
    
    return True, "All Steps", "Validation successful"
