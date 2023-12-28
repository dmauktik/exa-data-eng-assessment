"""Main module - Entry point for the program and validates commandline arguments.
The module instantiates ingest, transform and storage (ETL) modules as async tasks. """
import asyncio
import logging
from argparse import ArgumentParser
from ingest_fhir_records.fhir_reader import FhirReader
from transform_fhir_records.process_fhir import ProcessFihr
from store_fhir_records.store_fhir import StoreFhir

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', 
                    filename='transform_fhir.log', encoding='utf-8', level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')

def _parse_args():
    """Parses commandline arguments and validates user input.
    Input: None
    Returns: Provided command line args after validation"""
    arg_parser = ArgumentParser()
    arg_parser.add_argument("-m", "--mode", required=True, 
                           choices=['local_disk', 'get_file_url', 'get_folder_url'],
                           help="Source of fhir files to be processed")
    arg_parser.add_argument("-d", "--directory", required=False,
                           help="Local directory path where fhir files are stored")
    arg_parser.add_argument("-u;", "--url", required=False,
                           help="github url of fhir file for 'get_file_url'mode or github \
                            folder url for 'get_folder_url' mode")
    # Commandline argument validation
    args = arg_parser.parse_args()
    if args.mode == 'local_disk' and args.directory is None:
        arg_parser.error("Directory path is required with local_disk mode")
    if (args.mode == 'get_file_url' or args.mode == 'get_folder_url') and args.url is None:
        arg_parser.error("URL is required with get_file_url and get_folder_url")

    return args

async def main():
    """Main function to read command line arguments, validate them and call ETL modules.
    It is called by async event loop"""
    args =  _parse_args()
    reader = FhirReader()
    tasks = []
    #Instantiating ingest, transform and store modules (ETL) as async tasks
    match args.mode:
        case 'local_disk':
            logging.info("Mode: local_disk")
            tasks.append(asyncio.create_task(reader.local_dir_reader(args.directory)))
        case 'get_file_url':
            logging.info("Mode: get_file_url")
            tasks.append(asyncio.create_task(reader.url_file_reader(args.url)))
        case 'get_folder_url':
            logging.info("Mode: get_folder_url")
            tasks.append(asyncio.create_task(reader.url_directory_reader(args.url)))
    
    transform = ProcessFihr()
    tasks.append(asyncio.create_task(transform.process_bundle()))
    storage = StoreFhir()
    tasks.append(asyncio.create_task(storage.process_storage_queue_df()))
    await asyncio.gather(*tasks)
    logging.info("ETL task is complete!")

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())

# Command line examples:
# python main.py -m "local_disk" -d "C:\\Users\maukt\Documents\GitHub\exa-data-eng-assessment\data"
# python main.py -m "get_file_url" -u "https://raw.githubusercontent.com/dmauktik/exa-data-eng-assessment/main/data/Aaron697_Dickens475_8c95253e-8ee8-9ae8-6d40-021d702dc78e.json"    
# python main.py -m "get_folder_url" -u "https://github.com/dmauktik/exa-data-eng-assessment/tree/main/data"