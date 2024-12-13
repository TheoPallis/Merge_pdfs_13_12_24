import pandas as pd
import os
import timeit
import shutil
import logging
logging.basicConfig(filename='copy_folders.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

from datetime import datetime
# Function to get current timestamp
def current_timestamp():
    return datetime.now().strftime('%d-%m %H:%M:%S')

path = r"\\10.162.182.155\Florogoulas_PDF\Tzanetakos\Overdue\Sioufas"

folders = os.listdir(path)

for i,folder in enumerate(folders) :
    if (i+1) >= 1892 :
            root = os.path.join(path,folder)
            try :
                output_path = r"C:\Users\G.Kolokythas\Desktop\Downloaded dei\Sum"
                output_path = os.path.join(output_path,folder)
                start_time = timeit.default_timer()
                logging.info(f"Copying {i+1}/{len(folders)} {folder} ")
                print(f" Copying {i + 1}/{len(folders)}: {folder} [{current_timestamp()}]")

                shutil.copytree(root,output_path)
                end_time = timeit.default_timer()
                duration = end_time - start_time
                print(f"Copied {i+1}/{len(folders)} {folder} in {duration:.2f} seconds [{current_timestamp()}]")
                logging.info(f"Copied {i+1}/{len(folders)} {folder}")

            except Exception as e:
                if e == FileExistsError :

                    print(f"File {folder} already exists")
                    logging.warning(f"Folder {folder} already exists at the destination.")
                else :
                    print(f"Failed with error {e}")
                    logging.error(f"Failed to copy {folder} with error: {e}")
print("Operation complete!")

