import os
import asyncio
import pytest
from unittest.mock import patch

from ingest_fhir_records.fhir_reader import FhirReader

@pytest.fixture
def event_loop():
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
    assert result is None