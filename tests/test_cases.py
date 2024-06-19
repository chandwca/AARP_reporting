import unittest
from file_to_db import process_multiple_zips, extract_table_name
from db_config import HOST, DATABASE, HOST, PASSWORD,USER,DEV_USERNAME,DEV_PASSWORD,DEV_HOST,DEV_DATABASE
import os
from sqlalchemy import create_engine,text, inspect
import pandas as pd



class TestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # cls.zip_dir = '/Users/chetnachandwani/Documents/Projects/AARP/choice_zip_files'
        cls.zip_dir= '/Users/subashinibalasubramanian/Adroitts/AARP_Lifestyle/AARP_reporting/choice_zip_files'
        cls.data_frames = process_multiple_zips(cls.zip_dir)
        # cls.engine = create_engine(f"mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/{DATABASE}")
        cls.engine = create_engine(f"mssql+pyodbc://{DEV_USERNAME}:{DEV_PASSWORD}@{DEV_HOST}/{DEV_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server")
        cls.table_name = extract_table_name(os.listdir(cls.zip_dir)[0])
        cls.month_year = 'February_2024'
        cls.key_attributes = ['Customer_Hotel_ID', 'Check-In-Date', 'BM_Rate', 'AAA_Rate', 'BAR_Rate', 'LOY_Rate']
        cls.columns_to_check = ['BM_Rate', 'AAA_Rate', 'BAR_Rate', 'LOY_Rate']
        
    @classmethod
    def tearDownClass(cls):
        # Optional: Clean up resources after all tests in this class have run
        pass


class TestColumnCountComparison(TestBase):
    def test_column_count(self):
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(f"SELECT * FROM {self.table_name} WHERE 1=0"))
                db_column_count = len(result.keys())
                df_column_count = self.data_frames.shape[1]
                self.assertEqual(db_column_count, df_column_count, msg=f"Column counts does not match. File has {df_column_count} columns and table in DB has {db_column_count} columnss")
        except Exception as e:
            print(f"Exception occurred in test_column_count: {e}")
            raise


class TestDatatypesComparison(TestBase):
    def test_datatypes(self):
        try:
            with self.engine.connect() as connection:
                sql_df = pd.read_sql(f"SELECT * FROM {self.table_name}", connection)
                sql_dtypes = sql_df.dtypes.apply(lambda x: x.name).to_dict()
                # print("SQL Data Types:", sql_dtypes)
            df_dtypes = self.data_frames.dtypes.apply(lambda x: x.name).to_dict()
            # print("DataFrame Data Types:", df_dtypes)
            # Create a mapping function to handle dtype inconsistencies
            def normalize_dtype(dtype):
                if dtype == 'object':
                    return 'float64' 
                return dtype
            # Normalize data types for comparison
            df_dtypes_normalized = {col: normalize_dtype(dtype) for col, dtype in df_dtypes.items()}
            sql_dtypes_normalized = {col: normalize_dtype(dtype) for col, dtype in sql_dtypes.items()}

            for col, df_dtype in df_dtypes_normalized.items():
                sql_dtype = sql_dtypes_normalized.get(col)
                if sql_dtype:
                    self.assertEqual(df_dtype, sql_dtype, msg=f"Column {col} dtype mismatch: {df_dtype} != {sql_dtype}")
                else:
                    self.fail(f"Column {col} not found in SQL table")
            # Check for columns present in SQL but not in DataFrame
            for col in sql_dtypes_normalized:
                if col not in df_dtypes_normalized:
                    self.fail(f"Column {col} found in SQL table but not in DataFrame")
        except Exception as e:
            print(f"Exception occurred in test_datatypes: {e}")
            raise


class TestLowestRate(TestBase):
    def test_lowest_rate(self):
        try:
            query = f"SELECT * FROM {self.table_name};"
            with self.engine.connect() as connection:
                df = pd.read_sql(query, connection)
                # Convert columns to numeric
                numeric_columns = ['BM_Rate', 'AAA_Rate', 'BAR_Rate', 'LOY_Rate']  
                df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')
                # Find the lowest value for each record
                df['LowestValue'] = df[numeric_columns].min(axis=1)
                df['IsBMRateLowest'] = (df['BM_Rate'] == df['LowestValue'])
                lowest_values = df[numeric_columns].min(axis=1)
                isLowest = (df['BM_Rate'] == df['LowestValue'])
                # print(isLowest)
        except Exception as e:
            print(f"Exception occurred in test_lowest_rate: {e}")
            raise


class TestClosedRatesCheck(TestBase):
    def test_closed_rate(self):
        try:        
            query_template = """
            SELECT COUNT(*) AS count
            FROM {table_name}
            WHERE CAST([{column}] AS VARCHAR) = 'Closed'
            """
            columns =[]
            for column in self.columns_to_check:
                query = query_template.format(table_name=self.table_name, column=column)       
                with self.engine.connect() as connection:
                    result = connection.execute(text(query)).fetchone()
                    count = result[0]
                    if count > 0:
                        columns.append(column)
                    self.assertEqual(count, 0, msg=f"Rates contains closed value {columns}")       
        except Exception as e:
            print(f"Exception occurred in test_closed_rate: {e}")
            raise


class TestKeyAttributesNullCheck(TestBase):
    def test_key_attributes(self):
        try:
            null_columns = []
            null_count = 0
            query = f"SELECT * FROM {self.table_name};"
            with self.engine.connect() as connection:
                df = pd.read_sql(query, connection)
            for column in self.columns_to_check:
                null_count = null_count + df[column].isnull().sum() 
                if null_count > 0:
                    null_columns.append(column)
            self.assertEqual(len(null_columns), 0, msg=f"Columns {null_columns} contains null values") 
        except Exception as e:
            print(f"Exception occurred in test_key_attributes null check: {e}")
            raise




if __name__ == '__main__':
    unittest.main()
