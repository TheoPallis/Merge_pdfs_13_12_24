#10106031102 (pb & pod)
# 10113862701 (2 εξώδικα - επίδοση,μη επίδοση)
# Max downloaded 82632152603 
import pandas as pd
import os
import timeit
import shutil
import logging
import re
from collections import defaultdict
from datetime import datetime
from PyPDF2 import PdfReader, PdfWriter
logging.basicConfig(filename='final_folders_8_10.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to get current timestamp
def current_timestamp():
    return datetime.now().strftime('%d-%m %H:%M:%S')

# Define the main paths
path = r"C:\Users\G.Kolokythas\Desktop\Downloaded dei\Sum2"

def get_150_folders(path):
    df = pd.read_excel(r"C:\Users\G.Kolokythas\Downloads\Perimeter V2 (1) (2).xlsx", sheet_name='Processed Analysis', dtype=str)
    # scope = df[df['Θοδωρής'] == 'Θοδωρής 5_10']['Αρ. Παροχής']
    # scope = df[~df['POD'].isnull()]['Αρ. Παροχής']
    scope = df['Αρ. Παροχής']
    initial_scope_list = scope.tolist()
    filtered_scope_list = [x for x in initial_scope_list]# if int(x) <= int(82632152603)]
    # sorted_scope_list = sorted(filtered_scope_list)
    # print(sorted_scope_list)
    # print(sorted_scope_list.index('82630538702'))
    # print(len(sorted_scope_list))
    list_150 = set(filtered_scope_list)
    folders = os.listdir(path)
    folder_list_150 = [os.path.join(path,folder) for folder in folders if folder in list_150]
    pb_mapping_dict = df.set_index('Αρ. Παροχής')['PB'].to_dict()
    pod_df = pd.read_excel(r"C:\Users\G.Kolokythas\Downloads\Perimeter V2 (1) (2).xlsx", sheet_name= 'POD',dtype=str)
    pod_mapping_dict = pod_df.set_index('Αρ. Παροχής 11')['Αρ. Παροχής'].to_dict()
    return folder_list_150, pb_mapping_dict,pod_mapping_dict

folder_list_150, pb_mapping_dict,pod_mapping_dict = get_150_folders(path)
# # Returns none @check
exodika_mapping_dict = None#get_exodika_files(folder_list_150,exodika_path)

def copy_files(src, dest):
    """ Copy files from source to destination, creating destination directories if necessary. """
    if os.path.exists(src):
        os.makedirs(dest, exist_ok=True)
        for file in os.listdir(src):
            shutil.copy(os.path.join(src, file), os.path.join(dest, file))
            print(f"    (Fin) Copying {file}")

def delete_existing_merged_files(main_folder):
    for r,s,files in os.walk(main_folder):
        for file in files:
            if "merged_document.pdf" in file:
                os.remove(os.path.join(r,file))

def process_folder(folder_path,log_number):
    # Find all PDF files in the folder
    pdf_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.pdf')]
    # Extract the date from the pfd filename and sort the pdf files in each folder by date -> pdf_files_sorted
    pdf_files_sorted = sorted(pdf_files, key=lambda x: x.split("_")[-3])
    merged_pdf_path = os.path.join(folder_path, "merged_document.pdf")
    merge_pdfs(pdf_files_sorted, merged_pdf_path,log_number)

def merge_pdfs(pdf_list, output_path, log_number):
    """Merge multiple PDFs into a single PDF."""
    merged_document = PdfWriter()
    total_pages = 0

    for i, pdf in enumerate(pdf_list):
        with open(pdf, 'rb') as mfile:
            reader = PdfReader(mfile)
            pages = len(reader.pages)
            for page in reader.pages:
                merged_document.add_page(page)
            total_pages += pages
            logging.info(f"    Added {i + 1}/{len(pdf_list)} {pdf} with {pages} pages to the merged document {log_number} ({i + 1}/{len(pdf_list)})")

    with open(output_path, 'wb') as output_file:
        merged_document.write(output_file)

    logging.info(f"    Merged {len(pdf_list)} PDFs into {output_path} with {total_pages} pages")

def merge_script(folder):
    delete_existing_merged_files(folder)
    log_number = os.path.basename(folder)
    log_folder = os.path.join(folder,"Λογαριασμοί")
    print(log_number,log_folder)
    process_folder(log_folder,log_number)

def get_exodika_mapping_files() :
    exodika_path = r"C:\Users\G.Kolokythas\Desktop\Downloaded dei\150 batches files\Σιούφας Εξώδικα"
    exodika_mapping_dict = defaultdict(list)
    exodika_files = os.listdir(exodika_path)
    pattern = r'\b\d{11}'  # Adjusted to remove unnecessary escape characters
    for file in exodika_files:
        exodiko_codes = re.search(pattern, file).group(0)  # Extract digit sequences
        exodika_mapping_dict[exodiko_codes].append(os.path.join(exodika_path,file))
    return exodika_mapping_dict

exodika_mapping_dict = get_exodika_mapping_files()

def processor(folder_list_150, pb_mapping_dict,pod_mapping_dict,exodika_mapping_dict,start_index,end_index=10.000):
    output_base_path = r"C:\Users\G.Kolokythas\Desktop\Downloaded dei\150 batches files\Final2"
    bp_base_path = r"C:\Users\G.Kolokythas\Desktop\Downloaded dei\150 batches files\BP"
    pod_base_path = r"C:\Users\G.Kolokythas\Desktop\Downloaded dei\150 batches files\POD"

    for i, folder in enumerate(folder_list_150):
        # @ Filter by index
        # if i+1 >= 43 and i+1 <= 150  :
        # @ Filter by arithmo paroxis   
            try : 
                log_number = os.path.basename(folder) 
                # if log_number in ['10113862701'] :
    
                output_path = os.path.join(output_base_path, os.path.basename(folder))
                start_time = timeit.default_timer()
                logging.info(f"Copying {i+1}/{len(folder_list_150)} {folder}")
                print(f"(Fin) Copying {i + 1}/{len(folder_list_150)}: {folder} [{current_timestamp()}]")
        
                # # 1) Copy main folder
                shutil.copytree(folder, output_path)
                print(f"Copied exodika files")
                # # 2) Copy BP files
                bp = str(pb_mapping_dict[log_number])
                copy_files(os.path.join(bp_base_path, bp), os.path.join(output_path, "BP"))   
                print(f"Copied pb files")
                # 3) Copy POD files
                pod = str(pod_mapping_dict.get(log_number))
                copy_files(os.path.join(pod_base_path, pod), os.path.join(output_path, "POD"))
                print(f"Copieed pod files!")

                # 4) Copy exodika
                exodika_output_path = os.path.join(output_path,"Εξώδικα")
                os.makedirs(exodika_output_path, exist_ok=True)
                for exodiko in exodika_mapping_dict[log_number] :
                    shutil.copy(exodiko,os.path.join(exodika_output_path,os.path.basename(exodiko)))
                    print(f"Copied exopdiko {exodiko}")

                # 5) Merge log files
                merge_script(output_path)
                duration = timeit.default_timer() - start_time
                # print(f"(Fin) Copied {i+1}/{len(folder_list_150)} {folder} in {duration:.2f} seconds [{current_timestamp()}]")
                logging.info(f"Copied {i+1}/{len(folder_list_150)} {folder}")

                
            except FileExistsError:
                print(f"(Fin) Folder {folder} already exists")
                logging.warning(f"Folder {folder} already exists at the destination.")
            except Exception as e:
                print(f"(Fin) Failed with error: {e}")
                logging.error(f"Failed to copy {folder} with error: {e}")

processor(folder_list_150,pb_mapping_dict,pod_mapping_dict,exodika_mapping_dict,1,150)

# print("Operation complete!")

