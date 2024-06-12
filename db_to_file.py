import pandas as pd
from db_config import HOST, PORT, DATABASE, USER, PASSWORD
import mysql.connector

def read_table_to_file(conn, table_name, output_file, file_format):
    try:
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql_query(query, conn)
        if file_format == 'csv':
            output_file += '.csv'
            df.to_csv(output_file, index=False)
        elif file_format == 'txt':
            output_file += '.txt'
            df.to_csv(output_file, index=False, sep='|')
        else:
            print("Unsupported file format. Please choose 'csv' or 'txt'.")
            return
        print(f"Data from table {table_name} has been written to {output_file}")        
    except mysql.connector.Error as e:
        print("Error fetching data from the database:", e)

def main():
    conn = mysql.connector.connect(
        host=HOST,
        port=PORT,
        database=DATABASE,
        user=USER,
        password=PASSWORD
    )
    table_name = input("Enter the table name: ")
    output_file = input("Enter the output file name: ")
    file_format = input("Enter the file format (csv or txt): ").lower()
    read_table_to_file(conn, table_name, output_file, file_format)
    conn.close()

if __name__ == "__main__":
    main()
