import re
import pandas as pd
from db_config import HOST, DATABASE, HOST, PASSWORD,USER,DEV_USERNAME,DEV_PASSWORD,DEV_HOST,DEV_DATABASE
from sqlalchemy import create_engine,text
from constants import consistent_order, zip_dir, output_dir, table_name
import zipfile
import os


pd.set_option('future.no_silent_downcasting', True)
def preprocess_columns(columns):
    new_columns = []  
    for col in columns:
        if str(col[0]).startswith('Unnamed') and col[0] != '':
            col_name = str(col[1]).replace(' ', '_')
            new_columns.append(f"{col_name}")
        else:
            current = col[0].replace(' ', '_')
            if (col[0]== 'Occupancy'):
                new_columns.append(f"{col[1]}")
            elif (col[0]== 'Competitiveness'):
                new_columns.append(f"{col[1]}")
            elif (col[0]== 'Benchmark'):
                new_columns.append(f"BM_{col[1]}")
            elif '-BAR' in current:
                new_columns.append(f"BAR_{col[1]}")
            elif '-Loyality' in current:
                new_columns.append(f"LOY_{col[1]}")
            elif '-AAA' in current:
                new_columns.append(f"AAA_{col[1]}")
    # print(new_columns)
    return new_columns

def preprocess_excel(file_path):  
    df = pd.read_excel(file_path, sheet_name="Lowest_Rate_Report", header=[0, 1])
    df.columns = preprocess_columns(df.columns.to_flat_index())
    rate_columns = df.filter(regex='_Rate$').columns
    df[rate_columns] = df[rate_columns].replace("Closed", 9999.99)
    df = df.reindex(columns=consistent_order)
    return df

def insert_into_mysql(df,table_name):
    # engine = create_engine(f'mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/{DATABASE}')
    engine = f"mssql+pyodbc://{DEV_USERNAME}:{DEV_PASSWORD}@{DEV_HOST}/{DEV_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server"
    # df.to_sql("CHOICE", con=engine,schema='dbo', if_exists='replace', index=False)
    df.to_sql(table_name, con=engine, if_exists='replace', index=False) 
    print(f'Data inserted into {table_name} successfully.')

def extract_table_name(zip_filename):
    match = re.search(r'AARP_(.*?)_', zip_filename)
    if match:
        return match.group(1).upper()
    return None

def read_excel_files(output_dir):
    all_data_frames = []
    # Loop through the output directory to find Excel files
    for root, dirs, files in os.walk(output_dir):
        for file in files:
             # Check if the file name contains ~$
            if '~$' not in file:
                if file.endswith('.xlsx') or file.endswith('.xls'):
                    file_path = os.path.join(root, file)
                    df = preprocess_excel(file_path)
                    all_data_frames.append(df)
    if all_data_frames:
        combined_df = pd.concat(all_data_frames, ignore_index=True)
        combined_df = combined_df.drop_duplicates()
    return combined_df


def extract_files_from_zip(zip_dir):  
    for zip_file in os.listdir(zip_dir):
        if zip_file.endswith('.zip'):
            zip_path = os.path.join(zip_dir, zip_file)
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(output_dir)          
            except Exception as e:
                print(f"Failed to extract from {zip_file}: {e}")
    

def get_combined_df():
    extract_files_from_zip(zip_dir)
    combined_df = read_excel_files(output_dir)
    # insert_into_mysql(combined_df, table_name)
    return combined_df



def main():
    get_combined_df()
    print("DOne")





if __name__ == "__main__":
    main()


    