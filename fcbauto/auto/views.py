import os
import pandas as pd
import re
import dateparser
import numpy as np
from datetime import datetime, timedelta
from django.conf import settings
from django.core.files.storage import FileSystemStorage
from django.shortcuts import render
from .forms import ExcelUploadForm
from .mappings import consu_mapping, comm_mapping, guar_mapping, credit_mapping, prin_mapping,Gender_dict,Country_dict,state_dict,Marital_dict,Borrower_dict,Employer_dict,Title_dict,Occu_dict,sec_dict,AccountStatus_dict,Loan_dict,Repayment_dict,Currency_dict,Classification_dict,Collateraltype_dict,ConsuToComm
from rapidfuzz import fuzz, process
from typing import Union, Optional
from word2number import w2n  
from datetime import datetime
import traceback

def create_empty_sheet(mapping_dict):
    """
    Create an empty DataFrame with columns from the mapping dictionary
    """
    columns = list(mapping_dict.keys())
    return pd.DataFrame(columns=columns)

def ensure_all_sheets_exist(xds):
    """
    Check for missing sheets and create them with appropriate headers if needed
    """
    # Define expected sheets and their corresponding mappings
    expected_sheets = {
        'individualborrowertemplate': consu_mapping,
        'corporateborrowertemplate': comm_mapping,
        'creditinformation': credit_mapping,
        'guarantorsinformation': guar_mapping,
        'principalofficerstemplate': prin_mapping
    }
    
    processed_sheets = {}
    missing_sheets = []
    existing_sheets = []
    
    print("\n=== SHEET PROCESSING REPORT ===")
    print("Checking for required sheets...")
    
    for sheet_name, mapping in expected_sheets.items():
        # Clean the sheet name for comparison
        cleaned_name = clean_sheet_name(sheet_name)
        
        # Check if sheet exists in uploaded file
        sheet_exists = False
        for original_name in xds.keys():
            if clean_sheet_name(original_name) == cleaned_name:
                print(f"✓ Found existing sheet: {original_name}")
                processed_sheets[cleaned_name] = xds[original_name]
                sheet_exists = True
                existing_sheets.append(sheet_name)
                break
        
        # If sheet doesn't exist, create it
        if not sheet_exists:
            print(f"⚠ Missing sheet detected: {sheet_name}")
            print(f"→ Generating new sheet: {sheet_name}")
            print(f"  - Adding {len(mapping)} columns based on template")
            processed_sheets[cleaned_name] = create_empty_sheet(mapping)
            missing_sheets.append(sheet_name)
    
    # Summary report
    print("\n=== SHEET GENERATION SUMMARY ===")
    print(f"Total sheets required: {len(expected_sheets)}")
    print(f"Sheets found in upload: {len(existing_sheets)}")
    print(f"Sheets auto-generated: {len(missing_sheets)}")
    
    if missing_sheets:
        print("\nAuto-generated sheets:")
        for sheet in missing_sheets:
            print(f"- {sheet}")
    
    print("\n=============================")
    
    return processed_sheets


def clean_sheet_name(sheet_name):
    """Clean sheet names by removing special characters"""
    cleaned_name = re.sub(r'[^a-zA-Z0-9]', '', sheet_name)  
    return cleaned_name.lower()

def remove_special_characters(column_name):
    """Remove special characters and all spaces from column names"""
    # Remove non-alphanumeric characters but allow spaces
    pattern = r'[^a-zA-Z0-9]'  # Remove special characters
    cleaned_name = re.sub(pattern, '', column_name)  # Remove special characters
    
    # Remove all spaces
    cleaned_name = cleaned_name.replace(' ', '')  # Remove all spaces
    
    return cleaned_name

def remove_special_chars(text):
    """Remove special characters from text while preserving spaces"""
    if pd.isna(text) or text is None:  # Handle NaN and None values
        return ''
    if not isinstance(text, str):
        text = str(text)
    
    # Remove special characters but keep spaces and letters with accents
    cleaned = re.sub(r'[^a-zA-Z\s]', '', text)
    # Convert multiple spaces to single space and strip
    cleaned = ' '.join(cleaned.split())
    return cleaned

def remove_titles(name):
    """Remove common titles from names"""
    if not isinstance(name, str):
        return name
        
    titles = ['Miss', 'Mrs', 'Rev', 'Dr', 'Mr', 'MS', 'CAPT', 
              'COL', 'LADY', 'MAJ', 'PST', 'PROF', 'REV', 'SGT',
              'SIR', 'HE', 'JUDG', 'CHF', 'ALHJ', 'APOS', 'CDR',
              'BISH', 'FLT', 'BARR', 'MGEN', 'GEN', 'HON', 'ENGR', 'LT']
    
    pattern = r'\b(?:' + '|'.join(re.escape(title) for title in titles) + r')\b'
    cleaned_name = re.sub(pattern, '', name, flags=re.IGNORECASE)
    return ' '.join(cleaned_name.split())


def convert_date(date_string):
    """Converts a date string or Excel serial number to DD/MM/YYYY format"""
    if pd.isna(date_string) or date_string is None:
        return None

    # Convert to string if numeric
    if isinstance(date_string, (int, float)):
        try:
            # Excel date conversion
            base_date = datetime(1899, 12, 30)
            date = base_date + timedelta(days=int(date_string))
            return date.strftime('%d/%m/%Y')
        except (ValueError, OverflowError):
            return None

    if not isinstance(date_string, str):
        date_string = str(date_string)

    # Clean the string
    date_string = date_string.strip()
    
    # Handle empty strings and common missing values
    if not date_string or date_string.lower() in ['none', 'nan', 'nat', 'null', 'n/a', '']:
        return None

    try:
        # Try dateparser with specific formats first
        settings = {'DATE_ORDER': 'DMY'}  # Prefer day/month/year format
        parsed_date = dateparser.parse(date_string, settings=settings)
        
        if parsed_date:
            return parsed_date.strftime('%Y%m%d')
        
        return None
    except Exception:
        return None

def process_dates(df):
    """Process date fields in the DataFrame"""
    date_columns = [
        'DATEOFBIRTH',
        'DATEOFINCORPORATION',
        'PRINCIPALOFFICER1DATEOFBIRTH',
        'PRINCIPALOFFICER2DATEOFBIRTH',
        'SPOUSEDATEOFBIRTH',
        'GUARANTORDATEOFBIRTHINCORPORATION',
        'LOANEFFECTIVEDATE',
        'MATURITYDATE',
        'LASTPAYMENTDATE',
        'DEFEREDPAYMENTDATE',
        'LITIGATIONDATE',
        'ACCOUNTSTATUSDATE'
    ]
    
    for col in df.columns:
        # Check if column name contains 'date' (case insensitive)
        if 'date' in col.lower() or col in date_columns:
            print(f"Processing date column: {col}")  # Debug print
            try:
                df[col] = df[col].apply(convert_date)
                # Print sample of converted dates
                print(f"Sample of converted dates for {col}:")
                print(df[col].head())
            except Exception as e:
                print(f"Error processing dates in column {col}: {str(e)}")
    
    return df


def remove_titles(name):
    """Remove common titles from names"""
    if not isinstance(name, str):
        return ''
    
    titles = ['Miss', 'Mrs', 'Rev', 'Dr', 'Mr', 'MS', 'CAPT', 
              'COL', 'LADY', 'MAJ', 'PST', 'PROF', 'REV', 'SGT',
              'SIR', 'HE', 'JUDG', 'CHF', 'ALHJ', 'APOS', 'CDR',
              'BISH', 'FLT', 'BARR', 'MGEN', 'GEN', 'HON', 'ENGR', 'LT']
    
    pattern = r'\b(?:' + '|'.join(re.escape(title) for title in titles) + r')\b'
    cleaned_name = re.sub(pattern, '', name, flags=re.IGNORECASE)
    return ' '.join(cleaned_name.split())

def remove_special_chars(text):
    """Remove special characters from text while preserving spaces"""
    if not text:
        return ''
    
    # Convert to string if not already
    text = str(text)
    # Replace common punctuation with spaces
    text = re.sub(r'[.,\'"\-_/\\|&]', ' ', text)
    # Remove any remaining special characters but keep spaces
    text = re.sub(r'[^a-zA-Z\s]', '', text)
    # Replace multiple spaces with single space and strip
    text = ' '.join(text.split())
    
    return text.strip()

def process_names(df):
    """Process names before column mapping"""
    if df is None or df.empty:
        return df
        
    name_groups = {
        'primary': ['SURNAME', 'FIRSTNAME', 'MIDDLENAME'],
        'spouse': ['SPOUSESURNAME', 'SPOUSEFIRSTNAME', 'SPOUSEMIDDLENAME'],
        'principal1': ['PRINCIPALOFFICER1SURNAME', 'PRINCIPALOFFICER1FIRSTNAME', 'PRINCIPALOFFICER1MIDDLENAME'],
        'principal2': ['PRINCIPALOFFICER2SURNAME', 'PRINCIPALOFFICER2FIRSTNAME', 'PRINCIPALOFFICER2MIDDLENAME'],
        'guarantor': ['INDIVIDUALGUARANTORSURNAME', 'INDIVIDUALGUARANTORFIRSTNAME', 'INDIVIDUALGUARNTORMIDDLENAME']
    }
    
    for group_name, name_columns in name_groups.items():
        if all(col in df.columns for col in name_columns):
            # Debug print
            print(f"\nProcessing group: {group_name}")
            print("Original columns:", df[name_columns].head())
            
            # Explicitly clean columns
            for col in name_columns:
                # Convert to string, replace NaN with empty string
                df[col] = df[col].apply(lambda x: '' if x is None or (isinstance(x, float) and pd.isna(x)) else str(x).strip())
            
            # Print after initial cleaning
            print("After initial cleaning:", df[name_columns].head())
            
            # Remove titles and special characters
            for col in name_columns:
                df[col] = df[col].apply(remove_titles).apply(remove_special_chars)
            
            # Print after title and special char removal
            print("After title and special char removal:", df[name_columns].head())
            
            # Combine non-empty name components
            def combine_names(row):
                # Filter out empty strings before joining
                name_components = [
                    row[name_columns[0]], 
                    row[name_columns[1]], 
                    row[name_columns[2]]
                ]
                # Remove empty strings
                name_components = [comp for comp in name_components if comp]
                
                # Join non-empty components
                return ' '.join(name_components)
            
            temp_full_name = f'FULL_NAME_{group_name}'
            df[temp_full_name] = df.apply(combine_names, axis=1)
            
            # Print combined names
            print("Combined names:", df[temp_full_name].head())
            
            # Split the full name back into components
            name_parts = df[temp_full_name].apply(lambda x: pd.Series(x.split(maxsplit=2) + ['', '', ''])[:3])
            
            # Update original columns with processed parts
            df[name_columns[0]] = name_parts[0]
            df[name_columns[1]] = name_parts[1]
            df[name_columns[2]] = name_parts[2]
            
            # Print final processed columns
            print("Final processed columns:", df[name_columns].head())
            
            # Drop the temporary column
            df = df.drop(temp_full_name, axis=1)
        else:
            # Process individual columns if the full group is not present
            for col in name_columns:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: '' if x is None or (isinstance(x, float) and pd.isna(x)) else str(x).strip())
                    df[col] = df[col].apply(remove_titles).apply(remove_special_chars)
    
    return df

def rename_columns_with_fuzzy_rapidfuzz(df, mapping, threshold=99.5):
    """
    Rename DataFrame columns using fuzzy matching while preserving original columns
    that don't have matches and maintaining the order from mapping.
    """
    # Create a copy of the original DataFrame
    new_df = df.copy()
    
    # Print original columns
    print("\n=== COLUMN RENAMING PROCESS ===")
    print("Original columns:", df.columns.tolist())
    
    # Dictionary to store the new column names
    new_column_names = {}
    
    # Process each original column
    for original_col in df.columns:
        original_col_lower = original_col.lower()
        print(f"\nProcessing column: {original_col}")
        
        # First try exact matches
        exact_match = None
        for target_col, alternatives in mapping.items():
            if original_col_lower in [alt.lower() for alt in alternatives]:
                exact_match = target_col
                print(f"✓ Found exact match: {original_col} -> {target_col}")
                break
        
        if exact_match:
            new_column_names[original_col] = exact_match
            continue
            
        # If no exact match, try fuzzy matching
        best_match = None
        best_score = 0
        
        for target_col in mapping.keys():
            # Get the best match score among all alternatives for this target
            alternatives = mapping[target_col]
            scores = [fuzz.ratio(original_col_lower, alt.lower()) for alt in alternatives]
            max_score = max(scores) if scores else 0
            
            if max_score > best_score and max_score >= threshold:
                best_score = max_score
                best_match = target_col
                print(f"→ Found fuzzy match: {original_col} -> {target_col} (score: {max_score})")
        
        if best_match:
            new_column_names[original_col] = best_match
        else:
            print(f"! No match found for: {original_col}")
            new_column_names[original_col] = original_col
    
    # Rename the columns
    new_df = new_df.rename(columns=new_column_names)
    
    # Add any missing columns from the mapping with None values
    for target_col in mapping.keys():
        if target_col not in new_df.columns:
            new_df[target_col] = None
            print(f"+ Added missing column: {target_col}")
    
    # Reorder columns based on mapping
    new_df = new_df[list(mapping.keys())]
    
    # Print final columns
    print("\n=== FINAL COLUMN ORDER ===")
    print("Final columns:", new_df.columns.tolist())
    print("===========================\n")
    
    return new_df

def fill_data_column(df):
    """
    Fill the 'DATA' column with 'D' after column renaming
    """
    if 'DATA' in df.columns:
        print("\n=== DATA COLUMN PROCESSING ===")
        print("Found 'DATA' column - Filling with 'D'")
        df['DATA'] = 'D'
        print("✓ 'DATA' column filled successfully")
        print("===========================")
    else:
        print("\n=== DATA COLUMN NOT FOUND ===")
        print("'DATA' column is missing after renaming")
        print("===========================")
    
    return df

def process_gender(df):
    """Process gender fields in the DataFrame"""
    gender_columns = [
        'GENDER',
        'SPOUSEGENDER',
        'PRINCIPALOFFICER1GENDER',
        'PRINCIPALOFFICER2GENDER',
        'GUARANTORGENDER',
        'INDIVIDUALGUARANTORGENDER'
    ]
    
    for col in gender_columns:
        if col in df.columns:
            try:
                # Check if the column has any non-null values before processing
                if df[col].notna().any():
                    # Clean the values
                    df[col] = df[col].apply(lambda x: x.lower() if isinstance(x, str) else x)
                    df[col] = df[col].apply(lambda x: re.sub(r'[^a-zA-Z0-9]', '', x) if isinstance(x, str) else x)
                    
                    # Map the values using map_gender
                    df[col] = df[col].apply(map_gender)
                    
                    # Print confirmation that the column has been processed
                    print(f"Processed column '{col}':")
                    print(df[col].head())  # Show the first few rows of the processed column
                else:
                    print(f"No non-null values found in column '{col}'.")

            except Exception as e:
                print(f"Error processing column '{col}': {e}")
                print(f"Sample values in column '{col}': {df[col].head()}")
                print(f"Data type of column '{col}': {df[col].dtype}")
                print(f"Traceback: {traceback.format_exc()}")
    
    return df

def map_gender(value):
    """Maps gender values to standardized format"""
    if isinstance(value, pd.Series):  # Handle Series input
        return value.apply(map_gender)
    
    if pd.isna(value) or value is None:
        return None

    if not isinstance(value, str):
        value = str(value)

    value = value.lower().strip()
    
    if value in ['', 'none', 'nan', 'null', 'n/a']:
        return None

    for category, values in Gender_dict.items():
        if value in values:
            return category
    
    return None
def process_nationality(df):
    """Enhanced nationality processing with comprehensive error handling and .any() ambiguity resolution"""
    if df is None or df.empty:
        return df
    
    nationality_columns = [
        # Individual Borrower Template
        'NATIONALITY',
        'PRIMARYADDRESSCOUNTRY',
        'EMPLOYERCOUNTRY',
        'SECONDARYADDRESSCOUNTRY',
        
        # Corporate Borrower Template
        'BUSINESSOFFICEADDRESSCOUNTRY',
        'REGISTEREDADDRESSCOUNTRY',
        'COUNTRYOFINCORPORATION',
        'CORPORATECOUNTRY',
        
        # Principal Officers Template
        'PRINCIPALOFFICER1COUNTRY',
        'PRINCIPALOFFICER2COUNTRY',
        
        # Guarantor Information
        'GUARANTORSPRIMARYCOUNTRY',
        'GUARANTORSSECONDARYCOUNTRY'
    ]
    
    def clean_country_value(value):
        """Robust country value cleaning with detailed logging"""
        try:
            # Handle NaN or None values first
            if pd.isna(value) or value is None:
                return None
            
            # Convert to string safely
            value = str(value).strip()
            
            # Convert to lowercase and remove special characters
            value = value.lower()
            value = re.sub(r'[^a-zA-Z0-9\s]', '', value)
            
            # Check for empty or invalid values
            if not value or value in ['none', 'nan', 'null', 'na']:
                return None
            
            return value
        
        except Exception as e:
            print(f"Error cleaning country value '{value}': {e}")
            return None

    def standardize_country(value):
        """Enhanced country standardization with detailed logging"""
        if value is None:
            return None
        
        try:
            for standard_name, variations in Country_dict.items():
                if value in [v.lower() for v in variations]:
                    return standard_name
            
            return None
        
        except Exception as e:
            print(f"Error standardizing country '{value}': {e}")
            return None

    # Comprehensive column processing
    print("\n--- Nationality Column Processing ---")
    print("Available Columns:", df.columns.tolist())
    
    # Find columns that exist in the DataFrame
    found_columns = [col for col in nationality_columns if col in df.columns]
    print("Found Nationality Columns:", found_columns)
    
    for column in found_columns:
        print(f"\n=== Processing Column: {column} ===")
        
        # Detailed column information with .any() check
       # print(f"Column Data Type: {df[column].dtype}")
        print(f"Total Values: {len(df[column])}")
        print(f"Non-Null Values: {df[column].notna().sum()}")
        
        try:
            # Check if the column has any non-null values using .any()
            if df[column].notna().any():
                # Sample values before processing
                print("Sample Values (Before):")
                print(df[column].head())
                
                # Apply cleaning and standardization
                df[column] = df[column].apply(clean_country_value)
                df[column] = df[column].apply(standardize_country)
                
                # Sample values after processing
                print("Sample Values (After):")
                print(df[column].head())
                
                # Unique processed values
                print(f"Unique Values in {column}:")
                print(df[column].unique())
            else:
                print(f"SKIP: No non-null values in column {column}")
        
        except Exception as column_e:
            print(f"❌ FAILED to process column {column}: {column_e}")
            print(traceback.format_exc())
    
    return df

# def preserve_column_type(df):
#     """
#     Preserve column types for specific columns
    
#     Args:
#         df (pd.DataFrame): Input DataFrame
    
#     Returns:
#         pd.DataFrame: DataFrame with preserved column types
#     """
#     # Columns that should remain as strings
#     string_columns = [
#         'CUSTOMERID', 
#         'ACCOUNTNUMBER', 
#         'BRANCHCODE', 
#         'BVNNUMBER', 
#         'NATIONALIDENTITYNNUMBER',
#         'CUSTOMERSACCOUNTNUMBER'
#     ]
    
#     for col in string_columns:
#         if col in df.columns:
#             try:
#                 # Convert to string, preserving original value
#                 df[col] = df[col].astype(str)
                
#                 print(f"Converted column to string: {col}")
#                 print(f"Sample values: {df[col].head()}")
            
#             except Exception as e:
#                 print(f"Error converting {col} to string: {e}")
    
#     return df
# In your upload_file function, add this method to your processing pipeline
# Add it after initial processing, perhaps right after process_phone_columns

def process_special_characters(df):
    """Remove special characters from all columns except specified ones"""
    if df is None or df.empty:
        return df
    
    # List of columns to exclude from special character removal
    excluded_columns = [
        # Date columns
        'DATEOFBIRTH',
        'DATEOFINCORPORATION',
        'PRINCIPALOFFICER1DATEOFBIRTH',
        'PRINCIPALOFFICER2DATEOFBIRTH',
        'SPOUSEDATEOFBIRTH',
        'GUARANTORDATEOFBIRTHINCORPORATION',
        'LOANEFFECTIVEDATE',
        'MATURITYDATE',
        'LASTPAYMENTDATE',
        'DEFEREDPAYMENTDATE',
        'LITIGATIONDATE',
        'FACILITYTYPE',
        'CUSTOMERID',
        'ACCOUNTNUMBER',
        'CUSTOMERID',
        'BRANCHCODE',
        'BRANCH CODE'
        'CUSTOMERBRANCHUCODE',
        'CUSTOMERBRANCHCODE',
        'EMAIL',
        'EMAILADDRESS',
        'PRINCIPALOFFICER1EMAILADDRESS',
        'PRINCIPALOFFICER2EMAILADDRESS',
        'GUARANTOREMAIL',
        'OUTSTANDINGBALANCE',
        'MONTHLYREPAYMENT',
        'TOTALREPAYMENT',
        'CREDITLIMIT',
        'AVAILEDLIMIT',
        'OUTSTANDINGBALANCE',
        'CURRENTBALANCEDEBT',
        'INSTALMENTAMOUNT',
        'OVERDUEAMOUNT',
        'LASTPAYMENTAMOUNT',
        'ACCOUNTSTATUSDATE',
        'BVNNUMBER',
        'NATIONALIDENTITYNNUMBER'
    ]

    # Find processable columns (those not in excluded list)
    processable_columns = [col for col in df.columns if col not in excluded_columns]
    
    for column in processable_columns:
        # Safely apply the transformation
        try:
            # Check if the column has any non-null values before processing
            if df[column].notna().any():
                df[column] = df[column].apply(
                    lambda x: re.sub(r'[^a-zA-Z0-9]', ' ', str(x)) if pd.notnull(x) else x
                )
                # Remove double spaces
                df[column] = df[column].apply(lambda x: re.sub(r'\s+', ' ', x).strip() if isinstance(x, str) else x)
        except Exception as e:
            print(f"Error processing column {column}: {e}")
    
    return df

# Define the state columns
state_columns = [
    'STATE', 
    'PRIMARYADDRESSSTATE', 
    'SECONDARYADDRESSSTATE', 
    'EMPLOYERSTATE', 
    'BUSINESSOFFICEADDRESSSTATE', 
    'GUARANTORPRIMARYADDRESSSTATE', 
    'PRINCIPALOFFICER1STATE', 
    'PRINCIPALOFFICER2STATE'
]
# Define a function to perform fuzzy mapping
def fuzzy_map_state(state_name, state_dict):
    # Check if the state_name is empty or contains only whitespace
    if not state_name.strip():
        return None

    max_score = -1
    matched_state = None

    # Iterate through the state_dict and calculate fuzz ratio
    for state_code, names in state_dict.items():
        for name in names:
            score = fuzz.ratio(state_name.lower(), name.lower())
            if score > max_score:
                max_score = score
                matched_state = state_code

    # Define a threshold score (you can adjust this based on your requirements)
    threshold_score = 50

    # If the similarity score is above the threshold, return the corresponding state code
    if max_score >= threshold_score:
        return matched_state
    else:
        return None  # Return None if no good match is found

# Function to process state columns in the DataFrame
def process_states(consu):
    """Process state fields in the DataFrame"""
    for column in state_columns:
        if column in consu.columns and consu[column].apply(lambda x: not pd.isna(x) and str(x).strip() != '').any():
            # Clean and preprocess the column
            consu[column] = consu[column].apply(lambda x: str(x) if not pd.isna(x) else None)

            # Apply the fuzzy mapping function to non-empty values
            consu[column] = consu[column].apply(lambda x: fuzzy_map_state(x, state_dict) if not pd.isna(x) and str(x).strip() != '' else None)
        else:
            # No non-empty values found in the column, no action required
            pass

    return consu

def map_marital(value):
    if isinstance(value, str):
        for category, values in Marital_dict.items():
            if value in values:
                return category
    return None

def process_marital_status(df):
    """Process marital status fields in the DataFrame"""
    # Define the marital status columns to look for
    marital_columns = [
        'MARITALSTATUS',
        # Add any other relevant column names that may appear
    ]
    
    # Iterate through the list of potential marital status columns
    for col in marital_columns:
        if col in df.columns:
            # Clean the marital status values
            df[col] = df[col].apply(lambda x: x.lower() if isinstance(x, str) else x)
            df[col] = df[col].apply(lambda x: re.sub(r'[^a-zA-Z0-9]', '', x) if isinstance(x, str) else x)
            df[col] = df[col].apply(map_marital)
    
    return df

def map_borrowert(value):
    if isinstance(value, str):
        for category, values in Borrower_dict.items():
            if value in values:
                return category
    return None

def process_borrower_type(df):
    """Process borrower type fields in the DataFrame"""
    # Define the borrower type columns to look for
    borrower_columns = [
        'BORROWERTYPE'
        # Add any other relevant column names that may appear
    ]
    
    # Iterate through the list of potential borrower type columns
    for col in borrower_columns:
        if col in df.columns:
            # Clean the borrower type values
            df[col] = df[col].apply(lambda x: x.lower() if isinstance(x, str) else x)
            df[col] = df[col].apply(lambda x: re.sub(r'[^a-zA-Z0-9]', '', x) if isinstance(x, str) else x)
            df[col] = df[col].apply(map_borrowert)
    
    return df

def map_employers(value):
    if isinstance(value, str):
        for category, values in Employer_dict.items():
            if value in values:
                return category
    return None

def process_employment_status(df):
    """Process employment status fields in the DataFrame"""
    # Define the employment status columns to look for
    employment_columns = [
        'EMPLOYMENTSTATUS'
        # Add any other relevant column names that may appear
    ]
    
    # Iterate through the list of potential employment status columns
    for col in employment_columns:
        if col in df.columns:
            # Clean the employment status values
            df[col] = df[col].apply(lambda x: x.lower() if isinstance(x, str) else x)
            df[col] = df[col].apply(lambda x: re.sub(r'[^a-zA-Z0-9]', '', x) if isinstance(x, str) else x)
            df[col] = df[col].apply(map_employers)
    
    return df

def map_title(value):
    if isinstance(value, str):
        for category, values in Title_dict.items():
            if value in values:
                return category
    return None

def process_title(df):
    """Process title fields in the DataFrame"""
    # Define the title columns to look for
    title_columns = [
        'TITLE'
        # Add any other relevant column names that may appear
    ]
    
    # Iterate through the list of potential title columns
    for col in title_columns:
        if col in df.columns:
            # Clean the title values
            df[col] = df[col].apply(lambda x: x.lower() if isinstance(x, str) else x)
            df[col] = df[col].apply(lambda x: re.sub(r'[^a-zA-Z0-9]', '', x) if isinstance(x, str) else x)
            df[col] = df[col].apply(map_title)
    
    return df

def occu_title(value):
    if isinstance(value, str):
        for category, values in Occu_dict.items():
            if value in values:
                return category
        # If no match, check if the value is numeric
        if value.isdigit():
            return None  # Return None for numeric values
        # If the value is alphabetic, return it unchanged
        if value.isalpha():
            return value
    return None  # Return None for non-string types or unmatched cases

def process_occu(df):
    """Process title fields in the DataFrame"""
    # Define the title columns to look for
    occu_columns = [
        'OCCUPATION',
    ]
    
    # Iterate through the list of potential title columns
    for col in occu_columns:
        if col in df.columns:
            # Clean the title values
            df[col] = df[col].apply(lambda x: x.lower() if isinstance(x, str) else x)
            df[col] = df[col].apply(lambda x: re.sub(r'[^a-zA-Z0-9]', '', x) if isinstance(x, str) else x)
            df[col] = df[col].apply(occu_title)
    
    return df

def sec_title(value):
    if isinstance(value, str):
        # Check for matching values in the dictionary
        for category, values in sec_dict.items():
            if value in values:
                return category
        # If no match, check if the value is numeric
        if value.isdigit():
            return None  # Return None for numeric values
        # If the value is alphabetic, return it unchanged
        if value.isalpha():
            return value
    return None  # Return None for non-string types or unmatched cases

def process_business_sector(df):
    """Process business sector fields in the DataFrame"""
    # Define the business sector columns to look for
    sector_columns = [
        'BUSINESSSECTOR',
        # Add any other relevant column names that may appear
    ]
    
    # Iterate through the list of potential business sector columns
    for col in sector_columns:
        if col in df.columns:
            # Clean the business sector values
            df[col] = df[col].apply(lambda x: x.lower() if isinstance(x, str) else x)
            df[col] = df[col].apply(lambda x: re.sub(r'[^a-zA-Z0-9]', ' ', x) if isinstance(x, str) else x)
            df[col] = df[col].apply(sec_title)
    
    return df


def map_accountStatus(value):
    """Maps account status values to standardized format."""
    if pd.isna(value) or value is None:
        return None
    
    # Convert to string and clean
    value = str(value).lower()
    value = re.sub(r'[^a-zA-Z0-9]', '', value)
    
    for category, values in AccountStatus_dict.items():
        # Convert dictionary values to lowercase and remove special characters for comparison
        dict_values = [str(v).lower().replace(r'[^a-zA-Z0-9]', '') for v in values]
        if value in dict_values:
            return category
    return None  # Return None if no match is found

def clear_previous_info_columns(df):
    """
    Clear the contents of previous information columns while keeping headers
    """
    columns_to_clear = [
        'PREVIOUSACCOUNTNUMBER',
        'PREVIOUSNAME',
        'PREVIOUSCUSTOMERID',
        'PREVIOUSBRANCHCODE'
    ]
    
    print("\n=== CLEARING PREVIOUS INFO COLUMNS ===")
    for col in columns_to_clear:
        if col in df.columns:
            print(f"Clearing contents of {col}")
            df[col] = ''
    print("✓ Previous info columns cleared")
    print("================================")
    
    return df
def process_account_status(df):
    """Process account status fields in the DataFrame."""
    # Define the account status columns to look for
    status_columns = [
        'ACCOUNTSTATUS',
        'STATUS',  # Added alternative column name
        # Add any other relevant column names that may appear
    ]

    # Iterate through the list of potential account status columns
    for col in status_columns:
        if col in df.columns:
            print(f"Processing account status column: {col}")
            
            # Clean the account status values
            df[col] = df[col].apply(lambda x: x.lower() if isinstance(x, str) else x)
            df[col] = df[col].apply(lambda x: re.sub(r'[^a-zA-Z0-9]', '', x) if isinstance(x, str) else x)
            df[col] = df[col].apply(map_accountStatus)
            
            # Print unique values after processing
            print(f"Unique values in {col} after processing:", df[col].unique())
    
    return df
def exact_map_loan(loan_name):
    loan_name_lower = loan_name.lower()

    # Iterate through the Loan_dict and look for an exact match
    for loan_code, names in Loan_dict.items():
        if loan_name_lower in [name.lower() for name in names]:
            return loan_code

    # Return None if no exact match is found
    return None

def process_loan_type(df):
    """Process business sector fields in the DataFrame"""
    # Define the business sector columns to look for
    loan_columns = [
        'FACILITYTYPE',
        # Add any other relevant column names that may appear
    ]
    
    # Iterate through the list of potential business sector columns
    for col in loan_columns:
        if col in df.columns:
            # Clean the business sector values
            df[col] = df[col].apply(lambda x: x.lower() if isinstance(x, str) else x)
            df[col] = df[col].apply(lambda x: re.sub(r'[^a-zA-Z0-9]', '', x) if isinstance(x, str) else x)
            df[col] = df[col].apply(exact_map_loan)
    
    return df
def map_currency(value):
    """Maps currency values to standardized format."""
    if pd.isna(value) or value is None:
        return None
    
    # Convert to string and clean
    value = str(value).lower()
    value = re.sub(r'[^a-zA-Z0-9]', '', value)
    
    for category, values in Currency_dict.items():
        # Convert dictionary values to lowercase and remove special characters for comparison
        dict_values = [str(v).lower().replace(r'[^a-zA-Z0-9]', '') for v in values]
        if value in dict_values:
            return category
    return None   # Return None if no match is found

def process_currency(df):
    """Process currency fields in the DataFrame."""
    currency_columns = [
        'CURRENCY'
    ]
    
    for col in currency_columns:
        if col in df.columns:
            print(f"Processing currency column: {col}")
            
            # Clean the currency values
            df[col] = df[col].apply(lambda x: x.lower() if isinstance(x, str) else x)
            df[col] = df[col].apply(lambda x: re.sub(r'[^a-zA-Z0-9\s]', '', x) if isinstance(x, str) else x)  # Allow spaces
            df[col] = df[col].apply(map_currency)
            
            # Print unique values after processing
            print(f"Unique values in {col} after processing:", df[col].unique())
    
    return df

def map_repayment(value):
    """Maps repayment values to standardized format."""
    for category, values in Repayment_dict.items():
        if value in values:
            return category
    return None  # Return None if no match is found

def process_repayment(df):
    """Process repayment fields in the DataFrame."""
    repayment_columns = ['REPAYMENTFREQUENCY']  # Define the repayment columns to look for
    
    for col in repayment_columns:
        if col in df.columns:
            # Clean the repayment values
            df[col] = df[col].apply(lambda x: x.lower() if isinstance(x, str) else x)
            df[col] = df[col].apply(lambda x: re.sub(r'[^a-zA-Z0-9\s]', '', x) if isinstance(x, str) else x)  # Allow spaces
            df[col] = df[col].apply(map_repayment)
    
    return df
def map_collateraltype(value):
    for category, values in Collateraltype_dict.items():
        if value in values:
            return category
    return None

def process_collateral_type(df):
    """Process collateral type fields in the DataFrame."""
    collateral_columns = ['COLLATERALTYPE']  # Define the collateral type columns to look for
    
    for col in collateral_columns:
        if col in df.columns:
            # Clean the collateral type values
            df[col] = df[col].apply(lambda x: x.lower() if isinstance(x, str) else x)
            df[col] = df[col].apply(lambda x: re.sub(r'[^a-zA-Z0-9\s]', '', x) if isinstance(x, str) else x)  # Allow spaces
            df[col] = df[col].apply(map_collateraltype)
    
    return df
def map_classification(value):
    """Maps classification values to standardized format."""
    if pd.isna(value) or value is None:
        return None  # Return None for NaN or None values

    if not isinstance(value, str):
        value = str(value)  # Convert to string if not already

    # Check against the Classification_dict
    for category, values in Classification_dict.items():
        if value in values:
            return category  # Return the matched category

    return None 
def process_classification(df):
    """Process classification fields in the DataFrame."""
    classification_columns = ['LOANCLASSIFICATION']  # Define the classification columns to look for
    
    for col in classification_columns:
        if col in df.columns:
            # Clean the classification values
            df[col] = df[col].apply(lambda x: x.lower() if isinstance(x, str) else x)
            df[col] = df[col].apply(lambda x: re.sub(r'[^a-zA-Z0-9\s]', '', x) if isinstance(x, str) else x)  # Allow spaces
            df[col] = df[col].apply(map_classification)  # Apply the mapping function
    
    return df

def process_phone_columns(df):
    """
    Process numeric columns including telephone numbers
    """
    # Define columns that need numeric processing
    phone_columns = [
        'MOBILENUMBER', 'WORKTELEPHONE', 'HOMETELEPHONE', 
        'PRIMARYPHONENUMBER', 'SECONDARYPHONENUMBER',
        'PRINCIPALOFFICER1PHONENUMBER', 'PRINCIPALOFFICER2PHONENUMBER',
        'GUARANTORPRIMARYPHONENUMBER'
    ]
    
    try:
        if df is not None and not df.empty:
            # Process phone number columns
            for col in phone_columns:
                if col in df.columns:
                    print(f"Processing phone number column: {col}")
                    df[col] = df[col].astype(str)
                    # Remove any non-numeric characters
                    df[col] = df[col].apply(lambda x: ''.join(filter(str.isdigit, str(x))) if pd.notna(x) else '')
                    # Pad with zeros if less than 11 digits
                    df[col] = df[col].apply(lambda x: x.zfill(11) if x and len(x) < 11 else x)
                    # Replace 'nan' with empty string
                    df[col] = df[col].replace({'nan': ''})
                    print(f"Processed {col} - Sample values: {df[col].head().tolist()}")
    
    except Exception as e:
        print(f"Error in process_phone_columns: {e}")
        traceback.print_exc()
    
    return df

def convert_tenor_to_days(tenor: Union[str, int, float]) -> Optional[int]:
    """Converts the facility tenor to days.

    Args:
        tenor: The facility tenor string.

    Returns:
        The number of days in the tenor or the original number if no conversion is needed, or None if the input is invalid.
    """

    if tenor is None or tenor == '':
        return None

    # If the input is a number and doesn't need conversion, return it as it is
    if isinstance(tenor, (int, float)) and tenor <= 12:
        return int(tenor)

    tenor = str(tenor).strip().lower()  # Convert to string and remove leading/trailing spaces

    # Convert words to numbers if needed
    try:
        tenor = re.sub(r'(\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)', lambda x: str(w2n.word_to_num(x.group())), tenor)
    except ValueError:
        pass  # If conversion fails, assume it's already a number or contains units

    # Check if the input is just a number (integer or float) without units
    try:
        number = float(tenor)
        return int(number)
    except ValueError:
        pass

    # Match and separate the number and unit using regex, allowing optional spaces
    match = re.match(r'(\d+(\.\d+)?)\s*([a-z]+)', tenor)
    if not match:
        return None  # Invalid input format

    decimal_value, unit = match.group(1), match.group(3)

    try:
        decimal_value = float(decimal_value)
    except ValueError:
        return None  # Invalid decimal value

    # Convert decimal value to days based on unit
    if unit.startswith('m'):
        return int(decimal_value * 30)
    elif unit.startswith('d'):
        return int(decimal_value)
    elif unit.startswith('w'):
        return int(decimal_value * 7)
    elif unit.startswith('y'):
        return int(decimal_value * 365)
    else:
        return None  # Invalid unit

def process_loan_tenor(df):
    """
    Process loan tenor column in the DataFrame.
    Args:
        df: Input DataFrame
    Returns:
        DataFrame with processed loan tenor
    """
    if df is None:
        print("Input DataFrame is None.")
        return None

    if not isinstance(df, pd.DataFrame):
        print("Input is not a valid DataFrame.")
        return None

    # Columns to process for loan tenor
    tenor_columns = [ 'FACILITYTENOR']

    # Process each potential tenor column
    for col in tenor_columns:
        if col in df.columns:
            print(f"Processing column: {col}")

            # Apply conversion

            df[col] = df[col].apply(convert_tenor_to_days)
            # Convert to numeric, handling any conversion errors
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            df[col] = df[col].astype(str)
        else:
            print(f"Column {col} not found in DataFrame.")

    return df

def try_convert_to_float(x):
    try:
        return '{:.2f}'.format(float(x.replace(',', '').replace('-', '')) if x else '') 
    except (ValueError, AttributeError):
        return x

def process_numeric_columns(df):
    """Process numeric columns to standardize their format"""
    numeric_columns = [
        'AVAILEDLIMIT', 
        'CREDITLIMIT',
        'OVERDUEAMOUNT',
        'LASTPAYMENTAMOUNT',
        'INSTALMENTAMOUNT',
        'INCOME',
        'OUTSTANDINGBALANCE'
    ]
    
    for col in numeric_columns:
        if col in df.columns:
            print(f"Processing numeric column: {col}")
            
            # Apply the conversion function
            df[col] = df[col].apply(try_convert_to_float)
            
            # Optional: Convert to numeric, handling any remaining non-numeric values
            df[col] = pd.to_numeric(df[col], errors='coerce')
                        # Convert back to string, formatting NaN as empty string
            df[col] = df[col].apply(lambda x: '{:.2f}'.format(x) if pd.notnull(x) else '')

            # Print unique values after processing for verification
            print(f"Sample values in {col} after processing:")
            print(df[col].head())
    
    return df


def merge_dataframes(processed_sheets):
    """
    Merge different DataFrames from processed sheets
    
    Args:
        processed_sheets (dict): Dictionary of processed DataFrames
    
    Returns:
        tuple: (individual borrowers DataFrame, corporate borrowers DataFrame)
    """
    # Extract DataFrames from processed sheets
    consu = processed_sheets.get('individualborrowertemplate', pd.DataFrame())
    comm = processed_sheets.get('corporateborrowertemplate', pd.DataFrame())
    credit = processed_sheets.get('creditinformation', pd.DataFrame())
    guar = processed_sheets.get('guarantorsinformation', pd.DataFrame())
    prin = processed_sheets.get('principalofficerstemplate', pd.DataFrame())
    
    # Individual Borrowers Merge (previous implementation)
    indi = merge_individual_borrowers(consu, credit, guar)
    
    # Corporate Borrowers Merge
    corpo = merge_corporate_borrowers(comm, credit, prin)
    
    return indi, corpo

def merge_individual_borrowers(consu, credit, guar):
    """Merge individual borrower DataFrames"""
    # Validate DataFrames
    if consu.empty or credit.empty:
        print("Warning: Individual borrower or credit information DataFrame is empty")
        return pd.DataFrame()
    
    # Filter out rows with empty or blank 'CUSTOMERID'
    consu_cleaned = consu[
        consu['CUSTOMERID'].notna() & 
        (consu['CUSTOMERID'].str.strip() != '')
    ]
    
    # Merge attempts for individual borrowers
    try:
        # First, try inner merge on CUSTOMERID
        indi = pd.merge(
            consu_cleaned, 
            credit, 
            left_on='CUSTOMERID', 
            right_on='CUSTOMERID', 
            how='inner'
        )
    except Exception as e:
        print(f"First individual merge attempt failed: {e}")
        try:
            # Fallback: Left merge with alternative column matching
            indi = pd.merge(
                consu_cleaned, 
                credit, 
                left_on='CUSTOMERID', 
                right_on='ACCOUNTNUMBER', 
                how='left'
            )
        except Exception as e:
            print(f"Fallback individual merge attempt failed: {e}")
            return pd.DataFrame()
    
    # Merge with guarantor information
    try:
        indi = pd.merge(
            indi, 
            guar, 
            left_on='ACCOUNTNUMBER', 
            right_on='CUSTOMERSACCOUNTNUMBER', 
            how='left'
        )
    except Exception as e:
        print(f"Guarantor merge failed: {e}")
    
    return indi

def merge_corporate_borrowers(comm, credit, prin):
    """Merge corporate borrower DataFrames"""
    # Validate DataFrames
    if comm.empty or credit.empty:
        print("Warning: Corporate borrower or credit information DataFrame is empty")
        return pd.DataFrame()
    
    # Filter out rows with empty or blank 'CUSTOMERID'
    comm_cleaned = comm[
        comm['CUSTOMERID'].notna() & 
        (comm['CUSTOMERID'].str.strip() != '')
    ]
    
    # Merge attempts for corporate borrowers
    try:
        # First, try inner merge on CUSTOMERID
        corpo = pd.merge(
            comm_cleaned, 
            credit, 
            left_on='CUSTOMERID', 
            right_on='CUSTOMERID', 
            how='inner'
        )
    except Exception as e:
        print(f"First corporate merge attempt failed: {e}")
        try:
            # Fallback: Left merge with alternative column matching
            corpo = pd.merge(
                comm_cleaned, 
                credit, 
                left_on='CUSTOMERID', 
                right_on='ACCOUNTNUMBER', 
                how='left'
            )
        except Exception as e:
            print(f"Fallback corporate merge attempt failed: {e}")
            return pd.DataFrame()
    
    # Merge with principal officers information
    try:
        corpo = pd.merge(
            corpo, 
            prin, 
            left_on='CUSTOMERID', 
            right_on='CUSTOMERID', 
            how='left'
        )
    except Exception as e:
        print(f"Principal officers merge failed: {e}")
    
    return corpo
def remove_duplicates(df, identifier_columns=None):
    """
    Remove duplicates from DataFrame with flexible identifier handling
    
    Args:
        df (pd.DataFrame): Input DataFrame
        identifier_columns (list, optional): Columns to preserve unique entries
    
    Returns:
        pd.DataFrame: Cleaned DataFrame
    """
    if df is None or df.empty:
        return df
    
    # Potential identifier columns
    default_identifiers = [
        'CUSTOMERID', 
       # 'ACCOUNTNUMBER', 
      #  'LOANID', 
      #  'row_identifier'
    ]
    
    # Determine identifier columns
    if identifier_columns is None:
        identifier_columns = [
            col for col in default_identifiers 
            if col in df.columns
        ]
    
    # If no identifiers found, use all columns
    if not identifier_columns:
        print("No identifier columns found. Removing duplicates across all columns.")
        return df.drop_duplicates(keep='first')
    
    # Columns to check for duplicates (excluding identifiers)
    duplicate_check_columns = df.columns.difference(identifier_columns)
    
    # Remove duplicates
    df_cleaned = df.drop_duplicates(
        keep='first', 
        subset=duplicate_check_columns
    )
    
    # Log removed rows
    rows_removed = len(df) - len(df_cleaned)
    if rows_removed > 0:
        print(f"Removed {rows_removed} duplicate rows")
    
    return df_cleaned

def remove_duplicate_columns(df):
    """
    Remove duplicate columns, keeping first occurrence
    
    Args:
        df (pd.DataFrame): Input DataFrame
    
    Returns:
        pd.DataFrame: DataFrame with unique columns
    """
    if df is None or df.empty:
        return df
    
    # Identify unique columns
    unique_columns = []
    for col in df.columns:
        if col not in unique_columns:
            unique_columns.append(col)
    
    # Create DataFrame with unique columns
    df_cleaned = df[unique_columns]
    
    # Log column removals
    columns_removed = len(df.columns) - len(unique_columns)
    if columns_removed > 0:
        print(f"Removed {columns_removed} duplicate columns")
    
    return df_cleaned



# def merge_individual_borrowers(consu, credit, guar):
#     """Merge individual borrower DataFrames"""
#     try:
#         # First, try inner merge on CUSTOMERID
#         indi = pd.merge(
#             consu[consu['CUSTOMERID'].notna() & (consu['CUSTOMERID'].str.strip() != '')], 
#             credit, 
#             left_on='CUSTOMERID', 
#             right_on='CUSTOMERID', 
#             how='inner'
#         )
#     except Exception as e:
#         print(f"First individual merge attempt failed: {e}")
#         try:
#             # Fallback: Left merge with alternative column matching
#             indi = pd.merge(
#                 consu[consu['CUSTOMERID'].notna() & (consu['CUSTOMERID'].str.strip() != '')], 
#                 credit, 
#                 left_on='CUSTOMERID', 
#                 right_on='ACCOUNTNUMBER', 
#                 how='left'
#             )
#         except Exception as e:
#             print(f"Fallback individual merge attempt failed: {e}")
#             return pd.DataFrame()
    
#     # Merge with guarantor information
#     try:
#         indi = pd.merge(
#             indi, 
#             guar, 
#             left_on='ACCOUNTNUMBER', 
#             right_on='CUSTOMERSACCOUNTNUMBER', 
#             how='left'
#         )
#     except Exception as e:
#         print(f"Guarantor merge failed: {e}")
    
#     return indi

# def merge_corporate_borrowers(comm, credit, prin):
#     """Merge corporate borrower DataFrames"""
#     try:
#         corpo = pd.merge(
#             comm[comm['CUSTOMERID'].notna() & (comm['CUSTOMERID'].str.strip() != '')], 
#             credit, 
#             left_on='CUSTOMERID', 
#             right_on='CUSTOMERID', 
#             how='inner'
#         )
#     except Exception as e:
#         print(f"Corporate merge failed: {e}")
#         return pd.DataFrame()
#     try:
#         indi = pd.merge(
#             indi, 
#             prin, 
#             left_on='CUSTOMERID', 
#             right_on='CUSTOMERID', 
#             how='left'
#         )
#     except Exception as e:
#         print(f"Guarantor merge failed: {e}")
    
#     return corpo

def is_commercial_entity(name, commercial_keywords):
    """
    Check for commercial entities by looking at standalone words
    
    Args:
        name (str): Full name to check
        commercial_keywords (list): List of commercial keywords
    
    Returns:
        bool: True if likely a commercial entity, False otherwise
    """
    if not isinstance(name, str):
        return False
    
    # Convert to lowercase and split into words
    name_words = set(name.lower().split())
    
     # Convert keywords to lowercase for case-insensitive comparison
    commercial_keywords_lower = [keyword.lower() for keyword in commercial_keywords]
    # Check for standalone commercial keywords
    commercial_matches = [
        keyword for keyword in commercial_keywords_lower
        if keyword in name_words
    ]
    
    # Debug print for analysis
    if commercial_matches:
        print(f"Potential commercial entity detected: {name}")
        print(f"Matched standalone keywords: {commercial_matches}")
    
    return len(commercial_matches) > 0

def split_commercial_entities(indi):
    """
    Split commercial entities from individual borrowers
    
    Args:
        indi (pd.DataFrame): Individual borrowers DataFrame
    
    Returns:
        tuple: (Individual borrowers DataFrame, Commercial entities DataFrame)
    """
    # More comprehensive commercial keywords
    commercial_keywords = ['ltd', 'limited', 'solution', 'company', 'group', 'GROUP', 'school', 'sch', 'church', 'decor', 'union', 'organization',
                       'hospital', 'business', 'sons', 'college', 'primary', 'pry', 'secondary', 'global', 'rccg', 'service', 'associate',
                       'industry', 'industrial', 'enterprise', 'nigeria', 'solutions', 'project', 'tech', 'technical', 'resources', 'concept',
                       'concepts', 'schools', 'store', 'stores', 'bar', 'college', 'investment', 'pharmacy', 'parish', 'bank', 'microfinance',
                       'center', 'logistic', 'production', 'engineering', 'world', 'collection', 'furnitures', 'furniture', 'media',
                       'communication', 'accessories', 'academy', 'solutions', 'project', 'ventures', 'pharmaceuticals', 'enterprises',
                       'medical', 'centre', 'multiventures', 'academics', 'salon', 'spa', 'auto', 'sparepart', 'beautyspa',
                       'weavers', 'farmers', 'association', 'fashion', 'and', 'monuments', 'international', 'merchants', 'merchant',
                       'chambers', 'chamber', 'specialist', 'multi', 'consult', 'consulting', 'pavilion', 'fish', 'accessories', 'clothing',
                       'network', 'bootcamp', 'local', 'govt', 'government', 'ministry', 'gallery', 'premium', 'link', 'systems', 'system',
                       'integrated', 'event', 'management', 'catering', 'CONSUING', 'CONSUINGD', 'care', 'path', 'enterprises', 'energy',
                       'oil', 'gas', 'creative', 'workshop', 'cleaning', 'food', 'foods', 'bureau', 'business', 'research', 'country',
                       'state', 'property', 'estate', 'express', 'logistic', 'logistics', 'art', 'science', 'university', 'africa',
                       'technology', 'hotel', 'by', 'market', 'marketing', 'hair','markets', 'leasing', 'proventures', 'marine', 'laundry',
                       'wash', 'washing', 'cakes', 'deco', 'decoration', 'decorations', 'house', 'lightning', 'suites', 'suit', 'project',
                       'projects', 'lifestyle', 'designs', 'design', 'education', 'educational', 'agro', 'product', 'products', 'production',
                       'automobile', 'construction', 'constructions', 'associate', 'associates', 'cattle', 'restaurant', 'restaurant',
                       'resturant', 'resturants', 'consu', 'comm', 'kiddies','culture', 'couture', 'surveillance', 'security', 'rental', 'finance',
                       'plaza', 'motors', 'cuisine', 'interior', 'creamery', 'bakery', 'capital', 'partners', 'innovations', 'worldwide',
                       'petroleum', 'studios', 'studio', 'wholesale', 'medicare', 'textile', 'empire', 'army', 'ministries', 'services',
                       'consultants', 'cars', 'care', 'hireservices', 'data', 'venture', 'cupcake', 'chops', 'grills', 'grillz', 'smallchops'
                       ,'contractor','building','trading','nigeria','multivest','contribution','loan','youths','youth','consult','constructs']

    
    # Create a DataFrame to store commercial entities/
    corpo2 = pd.DataFrame(columns=indi.columns)
    
    # Rows to remove from individual borrowers
    rows_to_remove = []
    
    # Iterate through individual borrowers to find commercial entities
    for index, row in indi.iterrows():
        # Combine name columns for checking
        name_columns = ['SURNAME', 'FIRSTNAME', 'MIDDLENAME']
        full_name = ' '.join([str(row[col]).lower() for col in name_columns if pd.notna(row[col])])
        
        # Check if the name is a commercial entity
        if is_commercial_entity(full_name, commercial_keywords):
            # Prepare the row for commercial entities
            commercial_row = row.copy()
            
            # Combine names into SURNAME, drop other name columns
            commercial_row['SURNAME'] = f"{row['SURNAME']} {row['FIRSTNAME']} {row['MIDDLENAME']}".strip()
            commercial_row = commercial_row.drop(['FIRSTNAME', 'MIDDLENAME'])
            
            # Append to commercial entities
            corpo2 = pd.concat([corpo2, pd.DataFrame([commercial_row])], ignore_index=True)
            rows_to_remove.append(index)
    
    # Remove identified commercial entities from individual borrowers
    indi = indi.drop(rows_to_remove).reset_index(drop=True)
    
    # Debug prints
    print("Number of commercial entities found:", len(corpo2))
    print("Commercial entities columns:", corpo2.columns)
    print("First few commercial entities:")
    print(corpo2.head())
    
    return indi, corpo2
def merge_dataframes(processed_sheets):
    """
    Main merging function with sequential processing
    
    Args:
        processed_sheets (dict): Dictionary of processed DataFrames
    
    Returns:
        tuple: (Individual borrowers DataFrame, Corporate borrowers DataFrame)
    """
    commercial_keywords = ['ltd', 'limited', 'solution', 'company', 'group', 'GROUP', 'school', 'sch', 'church', 'decor', 'union', 'organization',
                       'hospital', 'business', 'sons', 'college', 'primary', 'pry', 'secondary', 'global', 'rccg', 'service', 'associate',
                       'industry', 'industrial', 'enterprise', 'nigeria', 'solutions', 'project', 'tech', 'technical', 'resources', 'concept',
                       'concepts', 'schools', 'store', 'stores', 'bar', 'college', 'investment', 'pharmacy', 'parish', 'bank', 'microfinance',
                       'center', 'logistic', 'production', 'engineering', 'world', 'collection', 'furnitures', 'furniture', 'media',
                       'communication', 'accessories', 'academy', 'solutions', 'project', 'ventures', 'pharmaceuticals', 'enterprises',
                       'medical', 'centre', 'multiventures', 'academics', 'salon', 'spa', 'auto', 'sparepart', 'beautyspa',
                       'weavers', 'farmers', 'association', 'fashion', 'and', 'monuments', 'international', 'merchants', 'merchant',
                       'chambers', 'chamber', 'specialist', 'multi', 'consult', 'consulting', 'pavilion', 'fish', 'accessories', 'clothing',
                       'network', 'bootcamp', 'local', 'govt', 'government', 'ministry', 'gallery', 'premium', 'link', 'systems', 'system',
                       'integrated', 'event', 'management', 'catering', 'CONSUING', 'CONSUINGD', 'care', 'path', 'enterprises', 'energy',
                       'oil', 'gas', 'creative', 'workshop', 'cleaning', 'food', 'foods', 'bureau', 'business', 'research', 'country',
                       'state', 'property', 'estate', 'express', 'logistic', 'logistics', 'art', 'science', 'university', 'africa',
                       'technology', 'hotel', 'by', 'market', 'marketing', 'hair','markets', 'leasing', 'proventures', 'marine', 'laundry',
                       'wash', 'washing', 'cakes', 'deco', 'decoration', 'decorations', 'house', 'lightning', 'suites', 'suit', 'project',
                       'projects', 'lifestyle', 'designs', 'design', 'education', 'educational', 'agro', 'product', 'products', 'production',
                       'automobile', 'construction', 'constructions', 'associate', 'associates', 'cattle', 'restaurant', 'restaurant',
                       'resturant', 'resturants', 'consu', 'comm', 'kiddies','culture', 'couture', 'surveillance', 'security', 'rental', 'finance',
                       'plaza', 'motors', 'cuisine', 'interior', 'creamery', 'bakery', 'capital', 'partners', 'innovations', 'worldwide',
                       'petroleum', 'studios', 'studio', 'wholesale', 'medicare', 'textile', 'empire', 'army', 'ministries', 'services',
                       'consultants', 'cars', 'care', 'hireservices', 'data', 'venture', 'cupcake', 'chops', 'grills', 'grillz', 'smallchops',
                       'contractor','building','trading','nigeria','multivest','contribution','loan','youths','youth','consult','constructs']

    # Extract DataFrames from processed sheets
    consu = processed_sheets.get('individualborrowertemplate', pd.DataFrame())
    comm = processed_sheets.get('corporateborrowertemplate', pd.DataFrame())
    credit = processed_sheets.get('creditinformation', pd.DataFrame())
    guar = processed_sheets.get('guarantorsinformation', pd.DataFrame())
    prin = processed_sheets.get('principalofficerstemplate', pd.DataFrame())
    
    # Step 1: Merge individual borrowers
    print("Starting the merging of individual borrowers with credit and guarantor information...")
    indi = merge_individual_borrowers(consu, credit, guar)
    print("Merging of individual borrowers with credit and guarantor information completed.")
    
    # Print column headers for individual borrowers
    print("Columns in merged individual borrowers DataFrame:")
    print(indi.columns.tolist())
    
    # Step 2: Merge corporate borrowers
    print("Starting the merging of corporate borrowers with credit and principal officer information...")
    corpo = merge_corporate_borrowers(comm, credit, prin)
    print("Merging of corporate borrowers with credit and principal officer information completed.")
    
    # Print column headers for corporate borrowers
    print("Columns in merged corporate borrowers DataFrame:")
    print(corpo.columns.tolist())
    
    # Step 3: Split commercial entities from individual borrowers
    indi, corpo2 = split_commercial_entities(indi)
    
    # Debug prints
    print("Original corporate borrowers:", len(corpo))
    print("Commercial entities to add:", len(corpo2))
    
    # Step 4: Rename commercial entities before combining
    if not corpo2.empty:
        # Rename corpo2 columns to match corporate borrower template
        corpo2 = rename_columns(corpo2, ConsuToComm.copy())
        
        # Debug statement to show corpo2 details before concatenation
        print("\nCommercial Entities (corpo2) Details:")
        print("Number of commercial entities:", len(corpo2))
        print("Columns in corpo2:", corpo2.columns)
        print("First few rows of corpo2:")
        print(corpo2.head())
        
        # Combine commercial entities with existing corporate borrowers
        corpo = pd.concat([corpo, corpo2], ignore_index=True)
        
        # Debug statement to confirm addition
        print("\nAfter Adding Commercial Entities:")
        print("Total corporate borrowers:", len(corpo))
        print("Columns in final corpo:", corpo.columns)
        print("First few rows after addition:")
        print(corpo.head())
        
        # Additional check to verify commercial entities were added
        commercial_entities_in_corpo = corpo[
            corpo['BUSINESSNAME'].apply(
                lambda x: any(keyword in str(x).lower() for keyword in commercial_keywords)
            )
        ]
        print("\nCommercial Entities in Final Corporate Borrowers:")
        print("Number of commercial entities:", len(commercial_entities_in_corpo))
        print("First few commercial entities:")
        print(commercial_entities_in_corpo.head())
    
    return indi, corpo
 
def rename_columns(df, column_mapping):
    """
    Rename columns based on a mapping dictionary
    
    Args:
        df (pd.DataFrame): Input DataFrame
        column_mapping (dict): Mapping of column names
    
    Returns:
        pd.DataFrame: DataFrame with renamed columns
    """
    try:
        # Print original columns before renaming
        print("Original columns before renaming:", list(df.columns))
        print("Mapping being used:", column_mapping)

        # Rename columns that match the mapping
        for column in list(df.columns):  # Use list() to create a copy of columns
            for mapped_column, alt_names in column_mapping.items():
                if column in alt_names or column.lower() in alt_names or column.upper() in alt_names:
                    df.rename(columns={column: mapped_column}, inplace=True)
                    print(f"Renamed {column} to {mapped_column}")
                    break
        
        # Print columns after initial renaming
        print("Columns after renaming:", list(df.columns))

        # Add missing columns from the dictionary
        for mapped_column in column_mapping.keys():
            if mapped_column not in df.columns:
                df[mapped_column] = None
                print(f"Added missing column: {mapped_column}")
        
        # Print columns before final reordering
        print("Columns before reordering:", list(df.columns))

        # Reorder the columns based on the keys in the dictionary
        df = df[list(column_mapping.keys())]
        
        # Print final columns
        print("Final columns after reordering:", list(df.columns))

        return df
    except Exception as e:
        print(f"Error in rename_columns: {e}")
        traceback.print_exc()
        return df

def modify_middle_names(df):
    """Keep only the first name in the specified middle name columns."""
    middle_name_columns = [
        'MIDDLENAME',
        'SPOUSEMIDDLENAME',
        'GUARANTORMIDDLENAME',
        'PRINCIPALOFFICER1MIDDLENAME',
        'PRINCIPALOFFICER2MIDDLENAME'
    ]
    
    for col in middle_name_columns:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: str(x).split()[0] if pd.notna(x) and str(x).strip() else '')
    
    return df


def upload_file(request):
    if request.method == 'POST':
        form = ExcelUploadForm(request.POST, request.FILES)
        if form.is_valid():
            uploaded_file = request.FILES['file']
            # Get original filename without extension
            original_filename = os.path.splitext(uploaded_file.name)[0]

            fs = FileSystemStorage()
            filename = fs.save(uploaded_file.name, uploaded_file)
            file_path = os.path.join(settings.MEDIA_ROOT, filename)
          
            try:
                # Read all sheets into a dictionary of DataFrames
                xds = pd.read_excel(file_path, sheet_name=None, na_filter=False, dtype=object)
                
                # Convert all sheets to string type immediately after reading
                for sheet_name, df in xds.items():
                    print(f"\nConverting sheet to string: {sheet_name}")
                    # Convert all columns to string
                    for col in df.columns:
                        df[col] = df[col].astype(str)
                        # Replace 'nan' values with empty string
                        df[col] = df[col].replace({'nan': '', 'None': '', 'NaN': ''})
                    xds[sheet_name] = df

                # Print initial sheet information
                print("\n=== INITIAL SHEET COUNT ===")
                print(f"Number of sheets in uploaded file: {len(xds)}")
                print("Sheets found:")
                for sheet_name in xds.keys():
                    print(f"- {sheet_name}")
                print("========================")

                # Ensure all required sheets exist
                processed_sheets = ensure_all_sheets_exist(xds)
                
                # Print processed sheet information
                print("\n=== PROCESSED SHEET COUNT ===")
                print(f"Number of sheets after processing: {len(processed_sheets)}")
                print("Final sheets:")
                for sheet_name in processed_sheets.keys():
                    print(f"- {sheet_name}")
                print("========================")

                # Process each sheet
                for sheet_name, sheet_data in xds.items():
                    cleaned_name = clean_sheet_name(sheet_name)
                    
                    # Process sheet
                    cleaned_df = sheet_data.copy()
                    for column in cleaned_df.columns:
                        if cleaned_df[column].dtype == object:
                            cleaned_df[column] = cleaned_df[column].apply(
                                lambda x: str(x)[:52] if isinstance(x, str) else x
                            )

                    cleaned_df.columns = [remove_special_characters(col) for col in cleaned_df.columns]
                    cleaned_df.columns = cleaned_df.columns.str.upper()

                    # Apply appropriate mapping based on sheet name
                    if cleaned_name == 'individualborrowertemplate':
                        cleaned_df = rename_columns_with_fuzzy_rapidfuzz(cleaned_df, consu_mapping)
                    elif cleaned_name == 'corporateborrowertemplate':
                        cleaned_df = rename_columns_with_fuzzy_rapidfuzz(cleaned_df, comm_mapping)
                    elif cleaned_name == 'principalofficerstemplate':
                        cleaned_df = rename_columns_with_fuzzy_rapidfuzz(cleaned_df, prin_mapping)
                    elif cleaned_name == 'creditinformation':
                        cleaned_df = rename_columns_with_fuzzy_rapidfuzz(cleaned_df, credit_mapping)
                    elif cleaned_name == 'guarantorsinformation':
                        cleaned_df = rename_columns_with_fuzzy_rapidfuzz(cleaned_df, guar_mapping)

                    # Apply processing steps
                    cleaned_df = process_dates(cleaned_df)
                    cleaned_df = process_special_characters(cleaned_df) 
                    cleaned_df = process_names(cleaned_df)
                    cleaned_df = process_nationality(cleaned_df)
                    cleaned_df = process_gender(cleaned_df)
                    cleaned_df = process_states(cleaned_df)
                    cleaned_df = process_marital_status(cleaned_df)
                    cleaned_df = process_borrower_type(cleaned_df)
                    cleaned_df = process_employment_status(cleaned_df)
                    cleaned_df = process_phone_columns(cleaned_df)
                    cleaned_df = process_title(cleaned_df)
                    cleaned_df = process_account_status(cleaned_df)
                    cleaned_df = process_loan_type(cleaned_df)
                    cleaned_df = process_currency(cleaned_df)
                    cleaned_df = process_repayment(cleaned_df)
                    cleaned_df = process_classification(cleaned_df)
                    cleaned_df = process_collateral_type(cleaned_df)
                    cleaned_df = process_loan_tenor(cleaned_df)
                    cleaned_df = clear_previous_info_columns(cleaned_df)
                    cleaned_df = process_numeric_columns(cleaned_df)
                    cleaned_df = fill_data_column(cleaned_df)

                    cleaned_df = remove_duplicate_columns(cleaned_df)
                    processed_sheets[cleaned_name] = cleaned_df

                # Merge processed sheets
                indi, corpo = merge_dataframes(processed_sheets)

                 # Now modify middle names after merging
                indi = modify_middle_names(indi)
                corpo = modify_middle_names(corpo)

                # Remove duplicates from merged DataFrames
                indi = remove_duplicates(indi)
                corpo = remove_duplicates(corpo)

                # Generate unique filenames with original filename
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                indi_output_filename = f"{original_filename}_individual_borrowers_{timestamp}.xlsx"
                corpo_output_filename = f"{original_filename}_corporate_borrowers_{timestamp}.xlsx"
                full_output_filename = f"{original_filename}_processed_{timestamp}.xlsx"

                # Paths for the output files
                indi_file_path = os.path.join(settings.MEDIA_ROOT, indi_output_filename)
                corpo_file_path = os.path.join(settings.MEDIA_ROOT, corpo_output_filename)
                full_file_path = os.path.join(settings.MEDIA_ROOT, full_output_filename)

                # Save individual borrowers merged file
                indi.to_excel(indi_file_path, index=False)
                indi_processed_file_url = fs.url(indi_output_filename)

                # Save corporate borrowers merged file
                corpo.to_excel(corpo_file_path, index=False)
                corpo_processed_file_url = fs.url(corpo_output_filename)

                # Save full processed file with all sheets
                with pd.ExcelWriter(full_file_path, engine='openpyxl') as writer:
                    # Save individual processed sheets
                    for sheet_name, df in processed_sheets.items():
                        print(f"Saving sheet: {sheet_name}")
                        df.to_excel(writer, sheet_name=sheet_name, index=False)

                    # Save merged sheets
                    if not indi.empty:
                        indi.to_excel(writer, sheet_name='Merged_Individual_Borrowers', index=False)
                    if not corpo.empty:
                        corpo.to_excel(writer, sheet_name='Merged_Corporate_Borrowers', index=False)
                full_processed_file_url = fs.url(full_output_filename)

                return render(request, 'upload.html', {
                    'form': form,
                    'success_message': 'File processed and merged successfully!',
                    'individual_download_url': indi_processed_file_url,
                    'corporate_download_url': corpo_processed_file_url,
                    'full_download_url': full_processed_file_url
                })

            except Exception as e:
                return render(request, 'upload.html', {
                    'form': form,
                    'error_message': f'Error processing file: {str(e)}'
                })
            finally:
                # Clean up the uploaded file
                if os.path.exists(file_path):
                    os.remove(file_path)

    else:
        form = ExcelUploadForm()
    
    return render(request, 'upload.html', {'form': form})
