import unittest
import pandas as pd
from file_to_db import process_multiple_zips, extract_table_name,preprocess_excel
from db_config import HOST, DATABASE, PASSWORD, USER, DEV_USERNAME, DEV_PASSWORD, DEV_HOST, DEV_DATABASE
import os
from constants import consistent_order
from sqlalchemy import create_engine, text

class TestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.zip_dir = '/Users/chetnachandwani/Documents/Projects/AARP/choice_zip_files'
        # cls.zip_dir= '/Users/subashinibalasubramanian/Adroitts/AARP_Lifestyle/AARP_reporting/choice_zip_files'
        cls.data_frames = process_multiple_zips(cls.zip_dir)
        cls.engine = create_engine(f"mssql+pyodbc://{DEV_USERNAME}:{DEV_PASSWORD}@{DEV_HOST}/{DEV_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server")
        cls.table_name = extract_table_name(os.listdir(cls.zip_dir)[0])
        cls.month_year = 'February_2024'
        cls.expected_first_five = {
            'BM_Rate': [9999.9899999999998, 9999.9899999999998],
            'BAR_Rate': [9999.9899999999998, 9999.9899999999998],
            'AAA_Rate': [9999.9899999999998, 9999.9899999999998],
            'LOY_Rate': [9999.9899999999998, 9999.9899999999998]
        }


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

# class TestColumnCountComparison(TestBase):
#     def test_column_count(self):
#         try:
#             query = text(f"SELECT * FROM {self.table_name} WHERE 1=0")
#             with self.engine.connect() as connection:
#                 result = connection.execute(query)
#                 db_column_count = len(result.keys())
#             df_column_count = self.data_frames[0].shape[1] if self.data_frames else 0
#             self.assertEqual(db_column_count, df_column_count, f"Column counts do not match")
#         except Exception as e:
#             print(e)

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
class TestRateValues(TestBase):
        def test_compare_rates(self):
            first_five_rows = self.data_frames.head(2)
            for i, row in first_five_rows.iterrows():
                with self.subTest(row=i):
                    self.assertAlmostEqual(row['BM_Rate'], self.expected_first_five['BM_Rate'][i], places=2,
                                        msg=f"BM_Rate for row {i} in DataFrame does not match expected value")
                    self.assertAlmostEqual(row['BAR_Rate'], self.expected_first_five['BAR_Rate'][i], places=2,
                                        msg=f"BAR_Rate for row {i} in DataFrame does not match expected value")
                    self.assertAlmostEqual(row['AAA_Rate'], self.expected_first_five['AAA_Rate'][i], places=2,
                                        msg=f"AAA_Rate for row {i} in DataFrame does not match expected value")
                    self.assertAlmostEqual(row['LOY_Rate'], self.expected_first_five['LOY_Rate'][i], places=2,
                                        msg=f"LOY_Rate for row {i} in DataFrame does not match expected value")
            db_values_query = text(f"""
            SELECT TOP 2 BM_Rate, BAR_Rate, AAA_Rate, LOY_Rate
            FROM {self.table_name}
        """)

            with self.engine.connect() as connection:
                result = connection.execute(db_values_query)
                db_rows = result.fetchall()  # Fetch all rows returned by the query

            for i, db_row in enumerate(db_rows):
                with self.subTest(row=i):
                    # Convert db_row values to float before comparison
                    db_bm_rate = float(db_row[0])
                    db_bar_rate = float(db_row[1])
                    db_aaa_rate = float(db_row[2])
                    db_loy_rate = float(db_row[3])

                    # Compare each rate with expected values
                    self.assertAlmostEqual(db_bm_rate, self.expected_first_five['BM_Rate'][i], places=2,
                                            msg=f"BM_Rate for row {i} in Database does not match expected value")
                    self.assertAlmostEqual(db_bar_rate, self.expected_first_five['BAR_Rate'][i], places=2,
                                            msg=f"BAR_Rate for row {i} in Database does not match expected value")
                    self.assertAlmostEqual(db_aaa_rate, self.expected_first_five['AAA_Rate'][i], places=2,
                                            msg=f"AAA_Rate for row {i} in Database does not match expected value")
                    self.assertAlmostEqual(db_loy_rate, self.expected_first_five['LOY_Rate'][i], places=2,
                                            msg=f"LOY_Rate for row {i} in Database does not match expected value")
        
if __name__ == '__main__':
    unittest.main()
