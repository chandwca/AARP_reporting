import pandas as pd
from db_config import HOST, DATABASE, SQL_DATABASE_NAME, SQL_HOST, SQL_PASSWORD, SQL_USERNAME, USER, PASSWORD
from sqlalchemy import create_engine
from constants import consistent_order
import zipfile
import os

output_dir = '/Users/subashinibalasubramanian/Adroitts/AARP_Lifestyle/AARP_reporting/extracted_files'


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

            # new_columns.append(f'{col[0]}_{col[1]}')
    print(new_columns)
    return new_columns


def preprocess_excel(file_path):
    
    df = pd.read_excel(file_path, sheet_name="Lowest_Rate_Report", header=[0, 1])
    df.columns = preprocess_columns(df.columns.to_flat_index())
    rate_columns = df.filter(regex='_Rate$').columns
    df[rate_columns] = df[rate_columns].replace("Closed", 9999.99)
    df = df.reindex(columns=consistent_order)
    print(df)
    return df


def insert_into_mysql(df):
    # change to mssql
    # engine = create_engine(f'mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/{DATABASE}')
    # engine = f"mssql+pyodbc://{connection_data.username}:{(connection_data.password)}@{connection_data.serverIP}:{connection_data.port}/{connection_data.databaseName}?driver=ODBC+Driver+17+for+SQL+Server"
    DRIVER_PATH = "/opt/homebrew/Cellar/msodbcsql17/17.10.6.1/lib/libmsodbcsql.17.dylib"
    engine = create_engine(f'mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_HOST}/{SQL_DATABASE_NAME}?driver=Microsoft+SQL+Server&Driver={DRIVER_PATH}')
    df.to_sql('western', con=engine, if_exists='replace', index=False)
    print(f'Data inserted into western successfully.')


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
    # print(os.listdir(zip_dir))
    for zip_file in os.listdir(zip_dir):
        if zip_file.endswith('.zip'):
            zip_path = os.path.join(zip_dir, zip_file)
            # print("zip path: ", zip_path)
            try:
                df = extract_from_excel(zip_path)
                all_data_frames.append(df)
            except Exception as e:
                print(f"Failed to process {zip_file}: {e}")
    if all_data_frames:
        combined_df = pd.concat(all_data_frames, ignore_index=True)
        print(combined_df)
        insert_into_mysql(combined_df)
    return all_data_frames


def main():
    # file_path = '/Users/chetnachandwani/Downloads/AARP_choice/Rate Report-2024-05-06-25158668.xlsx'
    # file_path = '/Users/subashinibalasubramanian/Documents/AARP/AARP_Choice_Brand AARP vs Brand other rataplan_OCTOBER_2023 2/Rate Report-2023-10-02-17495399.xlsx'
    # table_name = input("Enter the table name: ")
    # df = preprocess_excel(file_path)

    # insert_into_mysql(df, table_name)
    zip_dir = '/Users/subashinibalasubramanian/Adroitts/AARP_Lifestyle/AARP_reporting/western_zip_files'
    data_frames = process_multiple_zips(zip_dir)    

if __name__ == "__main__":
    main()
    
    
    
    # 1.combine the reports
    # 2."closed"-> 999.99
    