import unittest
from file_to_db import process_multiple_zips, extract_table_name
from db_config import HOST, DATABASE, HOST, PASSWORD,USER,DEV_USERNAME,DEV_PASSWORD,DEV_HOST,DEV_DATABASE
import os
from sqlalchemy import create_engine,text

class TestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.zip_dir = '/Users/chetnachandwani/Documents/Projects/AARP/choice_zip_files'
        # cls.zip_dir= '/Users/subashinibalasubramanian/Adroitts/AARP_Lifestyle/AARP_reporting/western_zip_files'
        cls.data_frames = process_multiple_zips(cls.zip_dir)
    @classmethod
    def tearDownClass(cls):
        # Optional: Clean up resources after all tests in this class have run
        pass
    
def get_record_count_for_month(data_frames, month_year):
    total_count = 0
    for df in data_frames:
        count = df[df['Report_Date'].dt.strftime('%B_%Y') == month_year].shape[0]
        total_count += count
    print(f"Total count of records for {month_year} across all DataFrames: {total_count}")
    return total_count

def get_record_count_for_month_db(table_name, month_year):
    engine = create_engine(f"mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/{DATABASE}")
    query = text(f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE DATE_FORMAT(Report_Date, '%M_%Y') = :month_year
    """)
    with engine.connect() as connection:
        result = connection.execute(query, {'month_year': month_year})
    row_count = result.fetchone()[0]
    
    return row_count

class TestRecordCountComparison(TestBase):
    
    def test_record_count_comparison(self):
        month_year = 'February_2024'
        df_count = get_record_count_for_month(self.data_frames, month_year)
        table_name = extract_table_name(os.listdir(self.zip_dir)[0])
        db_count = get_record_count_for_month_db(table_name, month_year)
        self.assertEqual(df_count, db_count, f"Record counts do not match for {month_year}: DataFrame count = {df_count}, DB count = {db_count}")
        print(f"Record counts match for {month_year}: DataFrame count = {df_count}, DB count = {db_count}")
        
if __name__ == '__main__':
    unittest.main()
