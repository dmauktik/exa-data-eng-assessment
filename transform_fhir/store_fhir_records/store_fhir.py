import os
import logging
from urllib.parse import quote_plus
from  sqlalchemy import create_engine
import pandas as pd
from  common.storage_queue import StorageQueue

logging.basicConfig(filename='transform_fhir.log', encoding='utf-8', level=logging.INFO)

class StoreFhir:
    def __init__(self) -> None:
        self.has_began = False
        self.database = None
        self.table_set = set()
        #TODO: change values to default and dummy
        db = os.environ.get('FHIR_DATABASE', 'fhirdata')
        dbuser = os.environ.get('DB_USERNAME', 'miku')
        dbpass = os.environ.get('DB_PASSWORD', 'nattu@123')
        dbpass = quote_plus(dbpass)
        dbhost = os.environ.get('DB_HOST', '127.0.0.1')
        dbport = os.environ.get('DB_PORT', '5432')
        self.connection_str = f'postgresql://{dbuser}:{dbpass}@{dbhost}:{dbport}/{db}'

    async def process_storage_df(self):
        """get fhir bundle from queue and flatten it"""
        engine = create_engine(self.connection_str)
        while True:
            # Wait for first dictionary to go in the queue
            if self.has_began is True and StorageQueue().queue_size() == 0:
                 break
            transact_dict = await StorageQueue().dequeue()
            self.has_began = True
            for k,df in transact_dict.items():
                df.to_sql(k, engine, index=False, if_exists='append')

                        



                


