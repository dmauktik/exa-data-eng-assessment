"""Main module - entry point"""
from argparse import ArgumentParser
from ingest_fhir_records.fhir_reader import FhirReader
from transform_fhir_records import process_fhir

def _parse_args():
    """Parse command line arguments and validate"""
    arg_parser = ArgumentParser()
    arg_parser.add_argument("-m", "--mode", required=True, 
                           choices=['local_disk', 'get_file_url', 'get_folder_url'],
                           help="Source of fhir files to be processed")
    arg_parser.add_argument("-d", "--directory", required=False,
                           help="Absolute local directory path where fhir files are stored")
    arg_parser.add_argument("-u;", "--url", required=False,
                           help="github url of fhir file for 'get_file_url'mode or github \
                            folder url for 'get_folder_url' mode")
    

    args = arg_parser.parse_args()
    if args.mode == 'local_disk' and args.directory is None:
        arg_parser.error("Directory path is required with local_disk mode")

    if (args.mode == 'get_file_url' or args.mode == 'get_folder_url') and args.url is None:
        arg_parser.error("URL is required with get_file_url and get_folder_url")
    
    return args

def main():
    """Get command line arguments and call ETL modules"""
    args =  _parse_args()
    reader = FhirReader()
    match args.mode:
        case 'local_disk':
            reader.local_dir_reader(args.directory)
        case 'get_file_url':
            reader.url_file_reader(args.url)
        case 'get_folder_url':
            reader.url_directory_reader(args.url)


if __name__ == '__main__':
    main()

# Command line:
# python main.py -m "local_disk" -d "C:\\Users\maukt\Documents\GitHub\exa-data-eng-assessment\data"
# python main.py -m "get_file_url" -u "https://raw.githubusercontent.com/dmauktik/exa-data-eng-assessment/main/data/Aaron697_Dickens475_8c95253e-8ee8-9ae8-6d40-021d702dc78e.json"    
# python main.py -m "get_folder_url" -u "https://github.com/dmauktik/exa-data-eng-assessment/tree/main/data"