import pandas as pd
import os
import sys

def extract_companies_from_index(index_file_path):
    """Generate a list of company files that need to be processed."""
    with open(index_file_path, "r") as company_file:
        contents = company_file.read()
    contents = contents.replace("'", "")
    contents_list = contents.split(",")
    cleaned_contents_list = [item.strip().replace('.csv', '') for item in contents_list]
    return cleaned_contents_list

def save_table(dataframe, output_path, file_name, header):
    """Saves an input pandas dataframe as a CSV file according to input parameters."""
    print(f"Path = {output_path}, file = {file_name}")
    dataframe.to_csv(output_path + file_name + ".csv", index=False, header=header)

def data_processing(source_data_path, output_path, index_file_path):
    """Process and collate company CSV file data into a single CSV file with additional summary columns."""
    
    def calculate_changes(df):
        df['daily_percent_change'] = ((df['close_value'] - df['open_value']) / df['open_value']) * 100
        df['value_change'] = df['close_value'] - df['open_value']
        return df
    
    company_names = extract_companies_from_index(index_file_path)
    dfs = []

    for company in company_names:
        file_path = os.path.join(source_data_path, f"{company}.csv")
        
        try:
            df = pd.read_csv(file_path)
            df['company_name'] = company  # Add company_name column
            df = calculate_changes(df)   # Calculate additional columns
            dfs.append(df)
        except Exception as e:
            print(f"Error processing {company} file: {str(e)}")
    
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        # Save to CSV without index and headers
        combined_df.to_csv(os.path.join(output_path, "historical_stock_data.csv"), index=False, header=False)
        print(f"Combined data saved to {os.path.join(output_path, 'historical_stock_data.csv')}")
    else:
        print("No valid data to save.")

if __name__ == "__main__":
    source_path = sys.argv[1]
    save_path = sys.argv[2]
    index_file_path = sys.argv[3]
    data_processing(source_path, save_path, index_file_path)
