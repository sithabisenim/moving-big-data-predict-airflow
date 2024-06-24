import pandas as pd
import os
import sys

def extract_companies_from_index(index_file_path):
    with open(index_file_path, "r") as company_file:
        contents = company_file.read().replace("'", "")
        contents_list = contents.split(",")
        cleaned_contents_list = [item.strip() for item in contents_list]
    return cleaned_contents_list

def calculate_changes(df):
    df['daily_percent_change'] = ((df['close_value'] - df['open_value']) / df['open_value']) * 100
    df['value_change'] = df['close_value'] - df['open_value']
    return df

def data_processing(source_path, save_path, index_file_path):
    companies = extract_companies_from_index(index_file_path)
    dfs = []

    for company in companies:
        file_path = os.path.join(source_path, f"{company}.csv")

        try:
            df = pd.read_csv(file_path)
            df['company_name'] = company
            df = calculate_changes(df)
            dfs.append(df)
        except Exception as e:
            print(f"Error processing {company} file: {str(e)}")

    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        output_file_path = os.path.join(save_path, "historical_stock_data.csv")
        combined_df.to_csv(output_file_path, index=False, header=False)
        print(f"Combined data saved to {output_file_path}")
    else:
        print("No valid data to save.")

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python data_processing.py <source_path> <save_path> <index_file_path>")
        sys.exit(1)

    source_path = sys.argv[1]
    save_path = sys.argv[2]
    index_file_path = sys.argv[3]
    data_processing(source_path, save_path, index_file_path)

