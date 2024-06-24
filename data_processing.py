import pandas as pd
import os
import sys

def extract_companies_from_index(index_file_path):
    """Generate a list of company files that need to be processed. 

    Args:
        index_file_path (str): path to index file

    Returns:
        list: Names of company names. 
    """
    with open(index_file_path, "r") as company_file:
        contents = company_file.read()
        contents = contents.replace("'", "")
        contents_list = contents.split(",")
        cleaned_contents_list = [item.strip() for item in contents_list]
    return cleaned_contents_list

def get_path_to_company_data(source_data_path):
    """Creates a list of the paths to the company data
       that will be processed

    Args:
        source_data_path (str): Path to where the company `.csv` files are stored. 

    Returns:
        list: List of paths to company CSV files.
    """
    path_to_company_data = []

    for file_name in os.listdir(source_data_path):
        if file_name.endswith(".csv"):
            path_to_company_data.append(os.path.join(source_data_path, file_name))
    
    return path_to_company_data

def save_table(dataframe, output_path, file_name, header):
    """Saves an input pandas dataframe as a CSV file according to input parameters.

    Args:
        dataframe (pandas.dataframe): Input dataframe.
        output_path (str): Path to which the resulting `.csv` file should be saved. 
        file_name (str): The name of the output `.csv` file. 
        header (boolean): Whether to include column headings in the output file.
    """
    output_file_path = os.path.join(output_path, file_name + ".csv")
    print(f"Path = {output_file_path}")
    dataframe.to_csv(output_file_path, index=False, header=header)

def data_processing(source_data_path, output_path, index_file_path):
    """Process and collate company CSV file data into a single CSV file with additional summary columns.

    Args:
        source_data_path (str): Path where the company CSV files are stored.
        output_path (str): Path to save the resulting CSV file ('historical_stock_data.csv').
        index_file_path (str): Path to the index file listing the companies to process.
    """
    def calculate_changes(df):
        df['daily_percent_change'] = ((df['close_value'] - df['open_value']) / df['open_value']) * 100
        df['value_change'] = df['close_value'] - df['open_value']
        return df
    
    dfs = []

    company_names = extract_companies_from_index(index_file_path)
    company_paths = get_path_to_company_data(source_data_path)
    
    for company in company_names:
        file_path = os.path.join(source_data_path, f"{company}.csv")
        try:
            df = pd.read_csv(file_path)
            df['company_name'] = company
            df = calculate_changes(df)
            dfs.append(df)
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}")
    
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        save_table(combined_df, output_path, "historical_stock_data", header=False)
        print(f"Combined data saved to {output_path}")
    else:
        print("No valid data to save.")

if __name__ == "__main__":
    source_path = sys.argv[1]
    save_path = sys.argv[2]
    index_file_path = sys.argv[3]
    data_processing(source_path, save_path, index_file_path)
