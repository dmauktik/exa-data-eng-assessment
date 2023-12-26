"""Module FhirReader reads json fhir format files (in the given Data folder) from 
local disk or from url as GET method. The file is parsed using fhir parser and the
parsed object is stored in the queue (for transform module to pick up and process)."""
from os import listdir
from os.path import isfile, join
import asyncio
import json
import logging
import aiofiles
import aiohttp
from requests.exceptions import HTTPError, RequestException, Timeout
from fhir.resources.R4B import construct_fhir_element
from common.fhir_queue import FhirQueue

logging.basicConfig(filename='transform_fhir.log', encoding='utf-8', level=logging.INFO)

class FhirReader:
    """The class ingestes FHIR records/files from local disk or from given URL as GET method"""
    def __init__(self, timeout=1000) -> None:
        # Hardcoding timeout for http request for now.
        # This should be a configurable value.
        self.timeout = timeout

    async def _add_to_queue(self, item):
        """Add bundle block to queue"""
        cmn = FhirQueue()
        await cmn.enqueue(item)

    async def _read_fhir_file(self, fil):
        """async file reader for local_dir_reader()"""
        data_json = {}
        try:
            data = None
            async with aiofiles.open(fil, mode='r', encoding='UTF-8') as fp:
                data = await fp.read()
            data_json = json.loads(data)
        except IOError as ex:
            logging.error(str(ex))
        except Exception as ex:
            logging.error("Unhandled exception due to: %s", str(ex))
        return data_json

    async def local_dir_reader(self, folder_path: str):
        """Method to read fhir bundle records from local disk and push to fhir queue"""
        response_val = False
        try:
            file_list = [join(folder_path, f) for f in listdir(folder_path) if isfile(join(folder_path, f))]
        except FileNotFoundError as ex:
            logging.error(str(ex))
            return False
        tasks = []
        for fp in file_list:
            tasks.append(asyncio.ensure_future(self._read_fhir_file(fp)))
        json_blocks = await asyncio.gather(*tasks)
        logging.info("Processing %d fhil bundles", len(json_blocks))
        for jblk in json_blocks:
            fhil_block = construct_fhir_element('Bundle', jblk)
            if fhil_block:
                await self._add_to_queue(fhil_block)
                response_val = True
            else:
                logging.error("None Fhir block object reveived as a response")
        logging.info("Queue size after ingestion is %d", FhirQueue().queue_size())
        return response_val

    async def _get_bundle_from_url(self, url: str, client: aiohttp.ClientSession) -> dict:
        """Internal method to call http get() method and return json response.
        Since github url is public, no authentication is required."""
        response_json = {}
        try:
            # if the url is secure, use auth= parameter below
            async with client.get(url) as response:
                if response.status == 200:
                    response_text = await response.text()
                    response_json = json.loads(response_text)
                else:
                    logging.error("Error calling url %s and error code is %d", url, response.status)
        except aiohttp.ClientConnectorError as ex:
            logging.error("Error calling url: %s", str(ex))
        except Exception as ex:
            logging.error("Error calling url: %s", str(ex))
        return response_json

    async def url_file_reader(self, url_to_call: str):
        """Method to GET single fhir bundle record from the given url and push to ingestion queue.
        The url should point to the file"""
        response_val = False
        async with aiohttp.ClientSession() as client:
            response_json = await self._get_bundle_from_url(url_to_call, client)
            if response_json:
                logging.info("Processing 1 fhil bundles")
                fhil_block = construct_fhir_element('Bundle', response_json)
                await self._add_to_queue(fhil_block)
                response_val = True
            else:
                logging.error("No response to process for %s", url_to_call)
            logging.info("Queue size after ingestion is %d", FhirQueue().queue_size())
            return response_val

    async def url_directory_reader(self, base_url: str):
        """Read github public url of directory (data directry), fetch file list 
        (assuming all are in json fhil format) and push a bundle record to fhir ingestion queue"""
        base_url = base_url + '/' if base_url[-1] != '/' else ''
        async with aiohttp.ClientSession() as client:
            response = await self._get_bundle_from_url(base_url, client)
            if response:
                try:
                    file_list = response["payload"]["tree"]["items"]
                except KeyError as ex:
                    logging.error("Error while getting file names: %s", str(ex))
                tasks = []
                #Fetch file contents in the directry in the loop
                for fl in file_list:
                    file_url = base_url  + fl["name"]
                    file_url = file_url.replace("/tree/", "/raw/")
                    tasks.append(asyncio.ensure_future(self._get_bundle_from_url(file_url, client)))
                
                logging.info("Fetching files from remote.....")
                response_jsons = await asyncio.gather(*tasks)
                for rj in response_jsons:
                    if rj:
                        fhil_block = construct_fhir_element('Bundle', rj)
                        await self._add_to_queue(fhil_block)
                    else:
                        logging.error("No response to process")

                logging.info("Queue size after extract process is %d", FhirQueue().queue_size())