import pandas as pd
from db_config import HOST, DATABASE, USER, PASSWORD
from sqlalchemy import create_engine

def preprocess_columns(columns):
    new_columns = []  
    for col in columns:
        print(type(col))
        if str(col[0]).startswith('Unnamed') and col[0] != '':
            print(col)
            new_columns.append(col[1])
        else:
            new_columns.append(f'{col[0]}_{col[1]}')
    return new_columns

def preprocess_excel(file_path):
    df = pd.read_excel(file_path, sheet_name="Lowest_Rate_Report", header=[0, 1])
    df.columns = preprocess_columns(df.columns.to_flat_index())
    rate_columns = df.filter(regex='_Rate$').columns
    df[rate_columns] = df[rate_columns].replace("Closed", 999.99)
    return df

def insert_into_mysql(df, table_name):
    # change to mssql
    engine = create_engine(f'mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/{DATABASE}')
    # engine = f"mssql+pyodbc://{connection_data.username}:{(connection_data.password)}@{connection_data.serverIP}:{connection_data.port}/{connection_data.databaseName}?driver=ODBC+Driver+17+for+SQL+Server"
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    print(f'Data inserted into {table_name} successfully.')

def main():
    file_path = '/Users/chetnachandwani/Downloads/AARP_choice/Rate Report-2024-05-06-25158668.xlsx'
    table_name = input("Enter the table name: ")
    df = preprocess_excel(file_path)
    insert_into_mysql(df, table_name)

if __name__ == "__main__":
    main()
    
    
    
    # 1.combine the reports
    # 2."closed"-> 999.99
    