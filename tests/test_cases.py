import unittest
import pandas as pd
from file_to_db import process_multiple_zips, extract_table_name,preprocess_excel
from db_config import HOST, DATABASE, PASSWORD, USER, DEV_USERNAME, DEV_PASSWORD, DEV_HOST, DEV_DATABASE
import os
from sqlalchemy import create_engine,text
import pandas as pd
from constants import consistent_order
import json

class TestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.zip_dir = '/Users/chetnachandwani/Documents/Projects/AARP/choice_zip_files'
        # cls.zip_dir= '/Users/subashinibalasubramanian/Adroitts/AARP_Lifestyle/AARP_reporting/choice_zip_files'
        cls.data_frames = process_multiple_zips(cls.zip_dir)
        cls.engine = create_engine(f"mssql+pyodbc://{DEV_USERNAME}:{DEV_PASSWORD}@{DEV_HOST}/{DEV_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server")
        cls.table_name = extract_table_name(os.listdir(cls.zip_dir)[0])
        cls.month_year = 'February_2024'
        cls.key_attributes = ['Customer_Hotel_ID', 'Check-In-Date', 'BM_Rate', 'AAA_Rate', 'BAR_Rate', 'LOY_Rate']
        cls.columns_to_check = ['BM_Rate', 'AAA_Rate', 'BAR_Rate', 'LOY_Rate']
        cls.expected_first_five = {
            'BM_Rate': [9999.9899999999998, 9999.9899999999998],
            'BAR_Rate': [9999.9899999999998, 9999.9899999999998],
            'AAA_Rate': [9999.9899999999998, 9999.9899999999998],
            'LOY_Rate': [9999.9899999999998, 9999.9899999999998]
        }
        with open('tests/expected_rates.json', 'r') as file:
            cls.expected_rates = json.load(file)

    def get_expected_rates(self, customer_hotel_id, check_in_date):
        for record in self.expected_rates:
            if record["Customer_Hotel_ID"] == customer_hotel_id and record["Check_In_Date"] == check_in_date:
                return record
        return None
    @classmethod
    def tearDownClass(cls):
        pass

def get_record_count_for_month(data_frame, month_year):
    total_count = data_frame[
        pd.to_datetime(data_frame['Report_Date']).dt.strftime('%B_%Y') == month_year
    ].shape[0]
    
    print(f"Total count of records for {month_year}: {total_count}")
    return total_count

def get_record_count_for_month_db(table_name, month_year):
    engine = create_engine(f"mssql+pyodbc://{DEV_USERNAME}:{DEV_PASSWORD}@{DEV_HOST}/{DEV_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server")
    query = text(f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE FORMAT(Report_Date, 'MMMM_yyyy') = :month_year
    """)
    with engine.connect() as connection:
        result = connection.execute(query, {'month_year': month_year})
        row_count = result.scalar()
    return row_count
class TestRecordCountComparison(TestBase):
    def test_record_count_comparison(self):
        df_count = get_record_count_for_month(self.data_frames, self.month_year)
        db_count = get_record_count_for_month_db(self.table_name, self.month_year)
        self.assertEqual(df_count, db_count, f"Record counts do not match for {self.month_year}: DataFrame count = {df_count}, DB count = {db_count}")
        print(f"Record counts match for {self.month_year}: DataFrame count = {df_count}, DB count = {db_count}")
class TestColumnHeadersComparison(TestBase):
    def test_column_headers_comparison(self):
        if not isinstance(self.data_frames, pd.DataFrame) or self.data_frames.empty:
            self.fail("No DataFrames to test")
        df_headers = self.data_frames.columns.tolist()
        query = text(f"SELECT * FROM {self.table_name} WHERE 1=0")
        with self.engine.connect() as connection:
            result = connection.execute(query)
            db_headers = result.keys()
        self.assertEqual(df_headers, list(db_headers), "Headers in DataFrame do not match headers in Database")
        self.assertEqual(df_headers, consistent_order, "Headers in DataFrame do not match expected headers from config file")
        print("Headers in DataFrame match the headers in Database and config file")
class TestDuplicateRecords(TestBase):
    def test_duplicate_records(self):
        duplicate_rows = self.data_frames[self.data_frames.duplicated()]
        df_total_duplicates = duplicate_rows.shape[0]
        print(f"Total duplicate records across all DataFrames: {df_total_duplicates}")
        db_total_duplicates = self.get_duplicate_count_in_db()
        self.assertEqual(df_total_duplicates, db_total_duplicates, f"Duplicate counts do not match: DataFrame count = {df_total_duplicates}, DB count = {db_total_duplicates}")
        self.assertEqual(df_total_duplicates, 0, f"There are {df_total_duplicates} duplicate records in the DataFrames")
        print("No duplicates found in both DataFrame and database.")

    def get_duplicate_count_in_db(self):
        query = text(f"""
            SELECT COUNT(*) AS duplicate_count
            FROM (
                SELECT Report_Date, Severity, Region, Country, City, Customer_Hotel_ID, Hotel_Name, [Check-In-Date], LOS, Adult, Child, Currency, [Tolerance%], Overall_Cheapest_Source, Cheapest_Competitor, [W/L/M], [Rate/Availability], Variance, [Variance%], BM_Rate, BM_Room_Type, BM_Board_Type, BM_Cancellation_Policy, BM_Shop_Time, AAA_Rate, AAA_Room_Type, AAA_Board_Type, AAA_Cancellation_Policy, [AAA_Variance(%)], AAA_ShopTime, BAR_Rate, BAR_Room_Type, BAR_Board_Type, BAR_Cancellation_Policy, [BAR_Variance(%)], BAR_ShopTime, LOY_Rate, LOY_Room_Type, LOY_Board_Type, LOY_Cancellation_Policy, [LOY_Variance(%)], LOY_ShopTime
                FROM {self.table_name}
                GROUP BY Report_Date, Severity, Region, Country, City, Customer_Hotel_ID, Hotel_Name, [Check-In-Date], LOS, Adult, Child, Currency, [Tolerance%], Overall_Cheapest_Source, Cheapest_Competitor, [W/L/M], [Rate/Availability], Variance, [Variance%], BM_Rate, BM_Room_Type, BM_Board_Type, BM_Cancellation_Policy, BM_Shop_Time, AAA_Rate, AAA_Room_Type, AAA_Board_Type, AAA_Cancellation_Policy, [AAA_Variance(%)], AAA_ShopTime, BAR_Rate, BAR_Room_Type, BAR_Board_Type, BAR_Cancellation_Policy, [BAR_Variance(%)], BAR_ShopTime, LOY_Rate, LOY_Room_Type, LOY_Board_Type, LOY_Cancellation_Policy, [LOY_Variance(%)], LOY_ShopTime
                HAVING COUNT(*) > 1
            ) AS duplicate_records
        """)
        with self.engine.connect() as connection:
            result = connection.execute(query)
            db_total_duplicates = result.scalar()
        return db_total_duplicates
    
def get_expected_rates(self, customer_hotel_id, check_in_date):
        for record in self.expected_rates:
            if record["Customer_Hotel_ID"] == customer_hotel_id and record["Check_In_Date"] == check_in_date:
                return record
        return None

# class TestRateValues(TestBase):
#     def test_compare_rates(self):
#         for i, row in self.data_frames.iterrows():
#             customer_hotel_id = row['Customer_Hotel_ID']
#             print("customer_hotel_id",customer_hotel_id)
#             check_in_date = row['Check-In-Date']
          

#         #     # Get expected rates from JSON
#             expected_rates = self.get_expected_rates(customer_hotel_id, check_in_date)
#             print("expected_rates")
#         #     self.assertIsNotNone(expected_rates, f"No matching row in JSON for Customer_Hotel_ID {customer_hotel_id} and Check_In_Date {check_in_date}")

#         #     # Compare DataFrame values
#         #     self.assertAlmostEqual(row['BM_Rate'], expected_rates['BM_Rate'], places=2, msg=f"BM_Rate in DataFrame does not match expected value for Customer_Hotel_ID {customer_hotel_id} and Check_In_Date {check_in_date}")
#         #     self.assertAlmostEqual(row['BAR_Rate'], expected_rates['BAR_Rate'], places=2, msg=f"BAR_Rate in DataFrame does not match expected value for Customer_Hotel_ID {customer_hotel_id} and Check_In_Date {check_in_date}")
#         #     self.assertAlmostEqual(row['AAA_Rate'], expected_rates['AAA_Rate'], places=2, msg=f"AAA_Rate in DataFrame does not match expected value for Customer_Hotel_ID {customer_hotel_id} and Check_In_Date {check_in_date}")
#         #     self.assertAlmostEqual(row['LOY_Rate'], expected_rates['LOY_Rate'], places=2, msg=f"LOY_Rate in DataFrame does not match expected value for Customer_Hotel_ID {customer_hotel_id} and Check_In_Date {check_in_date}")

#             # Fetch rates from the database for validation
#             db_values_query = text(f"""
#                 SELECT BM_Rate, BAR_Rate, AAA_Rate, LOY_Rate
#                 FROM {self.table_name}
#                 WHERE Customer_Hotel_ID = :customer_hotel_id AND Check_In_Date = :check_in_date
#             """)

#             with self.engine.connect() as connection:
#                 result = connection.execute(db_values_query, Customer_Hotel_ID=customer_hotel_id, Check_In_Date=check_in_date)
#                 db_row = result.fetchone()

#             self.assertIsNotNone(db_row, f"No matching row in Database for Customer_Hotel_ID {customer_hotel_id} and Check_In_Date {check_in_date}")

#             # Compare database values
#             db_bm_rate = float(db_row[0])
#             db_bar_rate = float(db_row[1])
#             db_aaa_rate = float(db_row[2])
#             db_loy_rate = float(db_row[3])

#             self.assertAlmostEqual(db_bm_rate, expected_rates['BM_Rate'], places=2, msg=f"BM_Rate for row in Database does not match expected value for Customer_Hotel_ID {customer_hotel_id} and Check_In_Date {check_in_date}")
#             self.assertAlmostEqual(db_bar_rate, expected_rates['BAR_Rate'], places=2, msg=f"BAR_Rate for row in Database does not match expected value for Customer_Hotel_ID {customer_hotel_id} and Check_In_Date {check_in_date}")
#             self.assertAlmostEqual(db_aaa_rate, expected_rates['AAA_Rate'], places=2, msg=f"AAA_Rate for row in Database does not match expected value for Customer_Hotel_ID {customer_hotel_id} and Check_In_Date {check_in_date}")
#             self.assertAlmostEqual(db_loy_rate, expected_rates['LOY_Rate'], places=2, msg=f"LOY_Rate for row in Database does not match expected value for Customer_Hotel_ID {customer_hotel_id} and Check_In_Date {check_in_date}")

class TestRateValues(TestBase):
    def test_compare_rates(self):
        for record in self.expected_rates:
            customer_hotel_id = record['Customer_Hotel_ID']
            check_in_date = record['Check_In_Date']

            
            db_values_query = text(f"""
                SELECT BM_Rate, BAR_Rate, AAA_Rate, LOY_Rate
                FROM {self.table_name}
                WHERE Customer_Hotel_ID = :customer_hotel_id AND [Check-In-Date] = :check_in_date
            """)

            with self.engine.connect() as connection:
                result = connection.execute(db_values_query, {'customer_hotel_id': customer_hotel_id, 'check_in_date': check_in_date})
                db_row = result.fetchone()

            self.assertIsNotNone(db_row, f"No matching row in Database for Customer_Hotel_ID {customer_hotel_id} and Check_In_Date {check_in_date}")

            # Compare database values
            db_bm_rate = float(db_row[0])
            db_bar_rate = float(db_row[1])
            db_aaa_rate = float(db_row[2])
            db_loy_rate = float(db_row[3])

            self.assertAlmostEqual(db_bm_rate, record['BM_Rate'], places=2, msg=f"BM_Rate for row in Database does not match expected value for Customer_Hotel_ID {customer_hotel_id} and Check_In_Date {check_in_date}")
            self.assertAlmostEqual(db_bar_rate, record['BAR_Rate'], places=2, msg=f"BAR_Rate for row in Database does not match expected value for Customer_Hotel_ID {customer_hotel_id} and Check_In_Date {check_in_date}")
            self.assertAlmostEqual(db_aaa_rate, record['AAA_Rate'], places=2, msg=f"AAA_Rate for row in Database does not match expected value for Customer_Hotel_ID {customer_hotel_id} and Check_In_Date {check_in_date}")
            self.assertAlmostEqual(db_loy_rate, record['LOY_Rate'], places=2, msg=f"LOY_Rate for row in Database does not match expected value for Customer_Hotel_ID {customer_hotel_id} and Check_In_Date {check_in_date}")

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
