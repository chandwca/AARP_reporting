import csv
from db_config import HOST, PORT, DATABASE, USER, PASSWORD
import mysql.connector

def read_table_to_csv(conn, table_name, output_file):
    try:
      
        cursor = conn.cursor()
        query = f"SELECT * FROM {table_name};"
        cursor.execute(query)
        rows = cursor.fetchall()
        with open(output_file, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow([desc[0] for desc in cursor.description])
            csv_writer.writerows(rows)
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
    read_table_to_csv(conn, table_name, output_file)
    conn.close()

if __name__ == "__main__":
    main()
