"""Module FhirReader to read fhir format files from local disk or from url as GET method"""
from os import listdir
from os.path import isfile, join
import json
import logging
from requests.exceptions import HTTPError, RequestException, Timeout
import requests
from fhir.resources.R4B import construct_fhir_element
from common import fhir_queue

# TODO: AsyncIO if time permits

logging.basicConfig(filename='transform_fhir.log', encoding='utf-8', level=logging.WARNING)

class FhirReader:
    """The class ingestes FHIR records/files from local disk or from given URL as GET method"""

    def __init__(self, timeout=500) -> None:
        self.timeout = timeout

    def _get_bundle_from_url(self, url: str) -> dict:
        """internal method to call get() method and return json response"""
        response_json = {}
        try:
            response = requests.get(url, timeout=self.timeout)
            response_json = response.json()
        except HTTPError as ex:
            logging.error("Error calling url: %s", str(ex))
        except Timeout as ex:
            logging.error("Timeout while calling url: %s", str(ex))
        except RequestException as ex:
            logging.error("Error calling url: %s", str(ex))
        return response_json

    def _add_to_queue(self, item):
        """Add bundle block to queue"""
        cmn = fhir_queue.FhirQueue()
        cmn.enqueue(item)

    def local_file_reader(self, folder_path: str):
        """Method to read fhir bundle records from local disk and push to ingestion queue"""
        try:
            file_list = [join(folder_path, f) for f in listdir(folder_path) if isfile(join(folder_path, f))]
        except FileNotFoundError as ex:
            logging.error(str(ex))
            return
        try:
            for fp in file_list:
                with open(fp, 'r', encoding='UTF-8') as f:
                    data = json.load(f)
                    fhil_block = construct_fhir_element('Bundle', data)
                    if fhil_block:
                        self._add_to_queue(fhil_block)
                    else:
                        logging.error("No response to process for %s", fp)
        except IOError as ex:
            logging.error(str(ex))
            return
        except LookupError as ex:
            logging.error(str(ex))
            return
        except Exception as ex:
            logging.error("Unhandled exception due to: %s", str(ex))

    def url_file_reader(self, url_to_call: str):
        """Method to GET fhir bundle record from the given url and push to ingestion queue"""
        response_json = self._get_bundle_from_url(url_to_call)
        if response_json:
            fhil_block = construct_fhir_element('Bundle', response_json)
            self._add_to_queue(fhil_block)
        else:
            logging.error("No response to process for %s", url_to_call)

    def directry_url_reader(self, base_url: str):
        """Read github public url of directory, fetch file list (assuming all are in fhil format)
         and push a bundle record to ingestion queue"""
        base_url = base_url + '/' if base_url[-1] != '/' else ''
        response = self._get_bundle_from_url(base_url)
        if response:
            try:
                file_list = response["payload"]["tree"]["items"]
            except KeyError as ex:
                logging.error("Error while getting file names: %s", str(ex))
            for fl in file_list:
                file_url = base_url + "/" + fl
                response_json = self._get_bundle_from_url(file_url)
                if response_json:
                    fhil_block = construct_fhir_element('Bundle', response_json)
                    self._add_to_queue(fhil_block)
                else:
                    logging.error("No response to process for %s", fl)