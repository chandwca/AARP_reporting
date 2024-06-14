# import unittest
# import os
# from file_to_db import process_multiple_zips, get_row_count_from_db

# class TestRowCount(unittest.TestCase):
    
   
#     def setUp(self):
#         self.zip_dir = '/Users/chetnachandwani/Documents/Projects/AARP/choice_zip_files'
    
#     def test_row_count(self):
        
#         total_rows, table_name = process_multiple_zips(self.zip_dir)
#         result=get_row_count_from_db(table_name)
#         print("hello")
#         print("result",result)
#         # if table_name:
#         #     db_row_count = get_row_count_from_db(table_name)
#         #     self.assertEqual(total_rows, db_row_count, "Row count mismatch between DataFrames and database table!")

#     def tearDown(self):
#         # Teardown code, if needed
#         pass

# if __name__ == '__main__':
#     unittest.main()

import unittest
# from my_package.module_b import SomeClass

class TestModuleB(unittest.TestCase):
    def test_some_class(self):
        # obj = SomeClass()
        self.assertEqual( "Hello from module_b")

if __name__ == '__main__':
    unittest.main()


