import re
import pandas as pd
from db_config import HOST, DATABASE, HOST, PASSWORD,USER,DEV_USERNAME,DEV_PASSWORD,DEV_HOST,DEV_DATABASE
from sqlalchemy import create_engine,text
from constants import consistent_order
import zipfile
import os

# output_dir ='/Users/chetnachandwani/Documents/Projects/AARP/extracted_files'
output_dir = '/Users/subashinibalasubramanian/Adroitts/AARP_Lifestyle/AARP_reporting/extracted_files'
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

def extract_from_excel(zip_path, extract_to='.'):
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file_info in zip_ref.infolist():       
                # Check if the file name contains ~$
                if '~$' not in file_info.filename:
                    # Extract the file
                    zip_ref.extract(file_info, output_dir)
                    file_path = os.path.join(output_dir, file_info.filename)
                    if file_info.filename.endswith('.xlsx') or file_info.filename.endswith('.xls'):
                        df = preprocess_excel(file_path)
                        return df
    except Exception as e:
        print(f"Exception: {e}")
    
def process_multiple_zips(zip_dir):
    all_data_frames = []
    table_name = None
    for zip_file in os.listdir(zip_dir):
        if zip_file.endswith('.zip'):
            zip_path = os.path.join(zip_dir, zip_file)
            try:
                if table_name is None:
                    table_name = extract_table_name(zip_file)
                    print(f"Extracted table name: {table_name}")
                df = extract_from_excel(zip_path)
                all_data_frames.append(df)
            except Exception as e:
                print(f"Failed to process {zip_file}: {e}")
    if all_data_frames and table_name:
        combined_df = pd.concat(all_data_frames, ignore_index=True)
        # insert_into_mysql(combined_df, table_name)
    return combined_df

def main():
    # zip_dir = '/Users/chetnachandwani/Documents/Projects/AARP/choice_zip_files'
    zip_dir = '/Users/subashinibalasubramanian/Adroitts/AARP_Lifestyle/AARP_reporting/choice_zip_files'
    process_multiple_zips(zip_dir)
if __name__ == "__main__":
    main()


    