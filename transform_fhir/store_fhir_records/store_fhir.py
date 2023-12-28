"""The module fetches transformed dataframe objects from Storage queue and
inserts in a database."""
import os
import logging
from urllib.parse import quote_plus
from  sqlalchemy import create_engine, text, exc
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
        """Fetch fhir bundle as dataframe from storage queue and insert into database
        Input: None
        Returns: Method execution status as boolean"""
        return_val = False
        engine = create_engine(self.connection_str)
        logging.info("Starting to get items from storage queue")
        while True:
            # Wait for the first dictionary object to go in the queue
            if self.has_began is True and StorageQueue().queue_size() == 0:
                 break
            transact_dict = await StorageQueue().dequeue()
            self.has_began = True
            print("Storage task picking next object...")
            # Bulk push the records in database. dtype for columns set to defaults i.e.
            # string object due to time constraint.
            try:
                for k,df in transact_dict.items():
                    df.to_sql(k, engine, index=False, if_exists='append')
                    with engine.connect() as con:
                        query = f"ALTER TABLE \"{k}\" ADD PRIMARY KEY (id);"
                        q_result = con.execute(text(query))
                        logging.debug(q_result)
                        con.commit()
                        return_val = True
            except exc.SQLAlchemyError as ex:
                logging.error("Error inserting records to database: %s", str(ex))
                return return_val
            logging.info("All records stored to database.")
            return return_val