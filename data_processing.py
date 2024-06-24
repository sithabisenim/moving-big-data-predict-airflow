import pandas as pd
import os
import sys
def extract_companies_from_index(index_file_path):
    """Generate a list of company names that need to be processed."""
    with open(index_file_path, "r") as company_file:
        contents = company_file.read()
        contents = contents.replace("'", "")
        contents_list = contents.split(",")
        cleaned_contents_list = [item.strip() for item in contents_list]
    return cleaned_contents_list

def get_path_to_company_data(list_of_companies, source_data_path):
    """Creates a list of paths to the company data files that will be processed."""
    path_to_company_data = []
    for file_name in list_of_companies:
        file_path = os.path.join(source_data_path, file_name)
        if os.path.exists(file_path):
            path_to_company_data.append(file_path)
        else:
            print(f"Warning: File not found at {file_path}")
    return path_to_company_data

def save_table(dataframe, output_path):
    """Saves a pandas dataframe as a CSV file."""
    dataframe.to_csv(output_path, index=False, header=False)

def standardize_columns(df):
    """Standardize column names to lower case and rename them."""
    df.columns = df.columns.str.lower()
    df = df.rename(columns={
        'date': 'stock_date',
        'open': 'open_value',
        'high': 'high_value',
        'low': 'low_value',
        'close': 'close_value',
        'volume': 'volume_traded'
    })
    return df

def enforce_schema(df):
    """Enforce the required schema and data types."""
    df['stock_date'] = pd.to_datetime(df['stock_date'], errors='coerce')
    df['open_value'] = pd.to_numeric(df['open_value'], errors='coerce')
    df['high_value'] = pd.to_numeric(df['high_value'], errors='coerce')
    df['low_value'] = pd.to_numeric(df['low_value'], errors='coerce')
    df['close_value'] = pd.to_numeric(df['close_value'], errors='coerce')
    df['volume_traded'] = pd.to_numeric(df['volume_traded'], errors='coerce').astype('Int64')
    df = df.dropna(subset=['stock_date', 'open_value', 'high_value', 'low_value', 'close_value', 'volume_traded'])
    return df

def data_processing(company_names, source_data_path, output_path):
    """Process and collate company CSV file data into a single CSV file with additional summary columns."""
    def calculate_changes(df):
        """Calculate daily percent change and value change."""
        df['daily_percent_change'] = ((df['close_value'] - df['open_value']) / df['open_value']) * 100
        df['value_change'] = df['close_value'] - df['open_value']
        return df
    
    dfs = []

    for company in company_names:
        file_path = os.path.join(source_data_path, company)
        
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                print(f"Skipping {company} as the file is empty.")
                continue

            df = standardize_columns(df)
            
            required_columns = ['stock_date', 'open_value', 'high_value', 'low_value', 'close_value', 'volume_traded']
            if not all(col in df.columns for col in required_columns):
                print(f"Skipping {company} due to missing required columns.")
                continue
            
            df = enforce_schema(df)
            df['company_name'] = company  # Add company_name column
            df = calculate_changes(df)   # Calculate additional columns
            
            df = df[['stock_date', 'open_value', 'high_value', 'low_value', 'close_value', 'volume_traded', 'daily_percent_change', 'value_change', 'company_name']]
            dfs.append(df)
        except pd.errors.EmptyDataError:
            print(f"Skipping {company} as the file is empty.")
        except pd.errors.ParserError:
            print(f"Skipping {company} due to parsing error.")
        except Exception as e:
            print(f"Error processing {company} file: {str(e)}")
    
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        save_table(combined_df, output_path)
        print(f"Combined data saved to {output_path}")
    else:
        print("No valid data to save.")

# Extract list of companies from index file
if __name__ == "__main__":
    import sys
    source_path = sys.argv[1]
    save_path = sys.argv[2]
    index_file_path = sys.argv[3]

    company_names = extract_companies_from_index(index_file_path)

    data_processing(company_names, source_path, save_path)
