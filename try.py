import os
import shutil
import pandas as pd

def preprocess_excel(file_path):
    # Your preprocess_excel function definition here
    # Example: return pd.read_excel(file_path)
    pass

def read_excel_files(output_dir):
    all_data_frames = []
    temp_dir = os.path.join(output_dir, 'temp')

    # Create a temporary directory if it doesn't exist
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    for root, dirs, files in os.walk(output_dir):
        for file in files:
            if '~$' not in file and (file.endswith('.xlsx') or file.endswith('.xls')):
                file_path = os.path.join(root, file)
                # Copy the file to the temporary directory
                temp_file_path = os.path.join(temp_dir, file)
                shutil.copy(file_path, temp_file_path)
                df = preprocess_excel(temp_file_path)
                all_data_frames.append(df)

    if all_data_frames:
        combined_df = all_data_frames[0]
        for df in all_data_frames[1:]:
            combined_df = pd.concat([combined_df, df])
        combined_df = combined_df.drop_duplicates()
        return combined_df
    
    # Clean up the temporary directory
    shutil.rmtree(temp_dir)

    return None
