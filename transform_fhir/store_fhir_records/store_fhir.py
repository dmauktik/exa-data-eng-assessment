"""The module fetches transformed dataframe objects from Storage queue and
stores in a database."""
import os
import json
import logging
from urllib.parse import quote_plus
from  sqlalchemy import create_engine, text, exc
import pandas as pd
from  common.storage_queue import StorageQueue

logging.basicConfig(filename='transform_fhir.log', encoding='utf-8', level=logging.INFO)

class StoreFhir:
    """StoreFhir class constructs database connection string, reads storage queue and stores
    transformed data in database. Tables are created dynamically. Data is in semi-structured 
    format and jsons are stored as string."""
    def __init__(self) -> None:
        self.has_began = False
        self.database = None
        self.table_set = set()
        # Dummy values as default
        db = os.environ.get('POSTGRES_DB', 'fhirdata')
        dbuser = os.environ.get('POSTGRES_USER', 'postgres')
        dbpass = os.environ.get('POSTGRES_PASSWORD', 'postgres')
        dbpass = quote_plus(dbpass)
        dbhost = os.environ.get('POSTGRES_HOST', '127.0.0.1')
        dbport = os.environ.get('POSTGRES_PORT', '5432')
        dbtype = os.environ.get('DB_TYPE', 'postgresql')
        self.connection_str = f'{dbtype}://{dbuser}:{dbpass}@{dbhost}:{dbport}/{db}'
        
    async def process_storage_queue_df(self):
        """Fetch fhir bundle as dataframe from storage queue and flatten it"""
        engine = create_engine(self.connection_str)
        while True:
            # Wait for first dictionary object to go in the queue
            if self.has_began is True and StorageQueue().queue_size() == 0:
                 break
            transact_dict = await StorageQueue().dequeue()
            self.has_began = True
            # Bulk push the data in database. dtype for columns set to defaults i.e. string
            # object due to time constraint.
            try:
                for k,df in transact_dict.items():
                    df.to_sql(k, engine, index=False, if_exists='append')
                    with engine.connect() as con:
                        query = f"ALTER TABLE \"{k}\" ADD PRIMARY KEY (id);"
                        result = con.execute(text(query))
                        con.commit()
            except exc.SQLAlchemyError as ex:
                logging.error("Error inserting records to database: %s", str(ex))
            return self.has_began


                


