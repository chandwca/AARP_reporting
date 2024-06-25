import re
import os
import zipfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from db_config import DEV_USERNAME, DEV_PASSWORD, DEV_HOST, DEV_DATABASE
from constants import consistent_order, zip_dir, output_dir, table_name

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ExcelProcessor") \
    .getOrCreate()

def preprocess_columns(columns):
    new_columns = []
    for col in columns:
        if str(col[0]).startswith('Unnamed') and col[0] != '':
            col_name = str(col[1]).replace(' ', '_')
            new_columns.append(f"{col_name}")
        else:
            current = col[0].replace(' ', '_')
            if col[0] == 'Occupancy':
                new_columns.append(f"{col[1]}")
            elif col[0] == 'Competitiveness':
                new_columns.append(f"{col[1]}")
            elif col[0] == 'Benchmark':
                new_columns.append(f"BM_{col[1]}")
            elif '-BAR' in current:
                new_columns.append(f"BAR_{col[1]}")
            elif '-Loyality' in current:
                new_columns.append(f"LOY_{col[1]}")
            elif '-AAA' in current:
                new_columns.append(f"AAA_{col[1]}")
    return new_columns

def preprocess_excel(file_path):
    df = spark.read.format("com.crealytics.spark.excel") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(file_path)
    df = df.toDF(*preprocess_columns(df.columns))
    rate_columns = [col for col in df.columns if col.endswith('_Rate')]
    for rate_col in rate_columns:
        df = df.withColumn(rate_col, col(rate_col).replace("Closed", 9999.99))
    df = df.select(*consistent_order)
    return df

def insert_into_mysql(df, table_name):
    df.write.format("jdbc") \
        .option("url", f"jdbc:sqlserver://{DEV_HOST};databaseName={DEV_DATABASE}") \
        .option("dbtable", table_name) \
        .option("user", DEV_USERNAME) \
        .option("password", DEV_PASSWORD) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .mode("overwrite") \
        .save()
    print(f'Data inserted into {table_name} successfully.')

def extract_table_name(zip_filename):
    match = re.search(r'AARP_(.*?)_', zip_filename)
    if match:
        return match.group(1).upper()
    return None

def read_excel_files(output_dir):
    all_data_frames = []
    for root, dirs, files in os.walk(output_dir):
        for file in files:
            if '~$' not in file and (file.endswith('.xlsx') or file.endswith('.xls')):
                file_path = os.path.join(root, file)
                df = preprocess_excel(file_path)
                all_data_frames.append(df)
    if all_data_frames:
        combined_df = all_data_frames[0]
        for df in all_data_frames[1:]:
            combined_df = combined_df.union(df)
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
    insert_into_mysql(combined_df, table_name)
    return combined_df

def main():
    get_combined_df()
    print("Done")

if __name__ == "__main__":
    main()
