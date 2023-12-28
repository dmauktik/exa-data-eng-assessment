"""Test module to unit test ingest, transform and storae functionalities"""
import os
import asyncio
import pytest
from asyncio.queues import QueueEmpty
from fhir.resources.R4B import construct_fhir_element
from transform_fhir_records.process_fhir import ProcessFihr
from ingest_fhir_records.fhir_reader import FhirReader
from store_fhir_records.store_fhir import StoreFhir
from common.fhir_queue import FhirQueue
from common.storage_queue import StorageQueue

@pytest.fixture
def event_loop():
    """Async eventloop for pytest"""
    loop = asyncio.get_event_loop()
    yield loop
    pending = asyncio.tasks.all_tasks(loop)
    loop.run_until_complete(asyncio.gather(*pending))
    loop.run_until_complete(asyncio.sleep(1))
    loop.close()

@pytest.mark.asyncio
async def test_local_dir_reader():
    """Function to test local_dir_reader() function"""
    result = await FhirReader().local_dir_reader(os.getcwd() + "\\tests\\data")
    assert result is True

@pytest.mark.asyncio
async def test_url_file_reader():
    """Function to test url_file_reader() function"""
    result = await FhirReader().url_file_reader("https://raw.githubusercontent.com/dmauktik/exa-data-eng-assessment/main/data/Aaron697_Dickens475_8c95253e-8ee8-9ae8-6d40-021d702dc78e.json")
    assert result is True

@pytest.mark.asyncio
async def test_negative_url_directory_reader():
    """Function to test url_directory_reader() function"""
    result = await FhirReader().url_directory_reader("https://example.com")
    assert result is False

@pytest.mark.asyncio
async def test_negative_process_bundle():
    """Function to test ProcessFihr.process_bundle() method. 
    test_url_file_reader() also covers process_bundle()"""
    encounter_data = {'resourceType': 'Bundle', 'type': 'transaction', 'entry': [{'fullUrl': 'test', 'resource': {}}]}
    fhil_block = construct_fhir_element('Bundle', encounter_data)
    await FhirQueue().enqueue(fhil_block)
    result = await ProcessFihr().process_bundle()
    assert False is result

@pytest.mark.asyncio
async def test_process_bundle():
    """Function to test ProcessFihr.process_bundle() method"""
    fhir_bundle = {
    "resourceType": "Bundle",
    "id": "bundle-example",
    "type": "searchset",
    "total": 1,
    "link": [
        {
            "relation": "self",
            "url": "https://example.com/fhir/Bundle?_page=1"
        }
    ],
    "entry": [
        {
            "fullUrl": "https://example.com/fhir/Patient/1",
            "resource": {
                "resourceType": "Patient",
                "id": "1",
                "name": [
                    {
                        "family": "Doe",
                        "given": ["John"]
                    }
                ],
                "gender": "male",
                "birthDate": "1980-01-01"
            },
            "request": {
                "method": "POST",
                "url": "Patient"
            }

        }
    ]
}
    size = FhirQueue().queue_size()
    fhil_block = construct_fhir_element('Bundle', fhir_bundle)
    await FhirQueue().enqueue(fhil_block)
    result = await ProcessFihr().process_bundle()
    assert size is size #1 record processed

async def test_process_storage_queue_df():
    """Function to test StoreFhir.process_storage_queue_df() method"""
    await StorageQueue().enqueue("test message")
    result = await StoreFhir().process_storage_queue_df()
    assert True is result # queue items accumulated from previous cases

# command: pytest -q .\tests\test_fhir.py
