import pandas as pd
import os
import timeit
import shutil
import logging
import re
from collections import defaultdict
from datetime import datetime
from PyPDF2 import PdfReader, PdfWriter
logging.basicConfig(filename='copy_folders.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# filter_value = x
# Function to get current timestamp
def current_timestamp():
    return datetime.now().strftime('%d-%m %H:%M:%S')

# Define the main paths
path = r"C:\Users\G.Kolokythas\Desktop\Downloaded dei\150 batches files\Final2"

def get_150_folders(path,filter_value):
    df = pd.read_excel(r"C:\Users\G.Kolokythas\Downloads\Perimeter_V2.xlsx", sheet_name='Processed Analysis', dtype=str)
    # scope = df[df['Θοδωρής'] == filter_value]['Αρ. Παροχής']
    scope = df[(df['Θοδωρής'].isna()) & (df["Ολοκληρωμένες Υποθέσεις"] == 'Έχει εκκινήσει η επεξεργασία')]['Αρ. Παροχής']

    initial_scope_list = scope.tolist()
    list_150 = set(initial_scope_list)
    folders = os.listdir(path)
    folder_list_150 = [os.path.join(path,folder) for folder in folders if folder in list_150]
    return folder_list_150

## CHANGE FILTER VALUE to date HERE -> 'Θοδωρής 9_10'
folder_list_150 = get_150_folders(path,'Θοδωρής 16_10')

def processor(folder_list_150):
    output_base_path = r"C:\Users\G.Kolokythas\Desktop\Downloaded dei\150 batches files\Upload"
    for i, folder in enumerate(folder_list_150):
            try : 
                log_number = os.path.basename(folder) 
                output_path = os.path.join(output_base_path, os.path.basename(folder))
                start_time = timeit.default_timer()
                logging.info(f"Copying {i+1}/{len(folder_list_150)} {folder}")
                print(f"(Fin) Copying {i + 1}/{len(folder_list_150)}: {folder} [{current_timestamp()}]")        
                # # 1) Copy main folder
                shutil.copytree(folder, output_path)
                print(f"(Fin) Copied {i + 1}/{len(folder_list_150)}: {folder} [{current_timestamp()}]")       
                
            except FileExistsError:
                print(f"(Fin) Folder {folder} already exists")
                logging.warning(f"Folder {folder} already exists at the destination.")
            except Exception as e:
                print(f"(Fin) Failed with error: {e}")
                logging.error(f"Failed to copy {folder} with error: {e}")

processor(folder_list_150)

# print("Operation complete!")

