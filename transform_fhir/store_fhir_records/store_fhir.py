"""The module fetches transformed dataframe objects from Storage queue and
inserts in a database."""
import os
import logging
import time
from urllib.parse import quote_plus
from  sqlalchemy import create_engine, text, exc
from  common.storage_queue import StorageQueue

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', 
                    filename='transform_fhir.log', encoding='utf-8', level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')

class StoreFhir:
    """StoreFhir class constructs database connection string, reads storage queue and stores
    transformed data in database. Tables are created dynamically. Data is in semi-structured 
    format and jsons are stored as string."""
    def __init__(self) -> None:
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
        self.tablecols = {}
        
    async def process_storage_queue_df(self):
        """Fetch fhir bundle as dataframe from storage queue and insert into database
        Input: None
        Returns: Method execution status as boolean"""
        return_val = True
        engine = create_engine(self.connection_str)
        logging.info("Starting to get items from storage queue")
        set_pkey_once = False
        while True:
            transact_dict = await StorageQueue().dequeue()
            if transact_dict is None:
                break
            print("Storage task picking next object...")
            # Push a bundle of records in database. dtype for columns set to defaults i.e.
            # string object due to time constraint.
            try:
                for k,df in transact_dict.items():
                    if k not in self.tablecols:
                        self.tablecols[k] = list(df.columns)
                    else:
                        temp_colist = list(df.columns)
                        col_to_add = [item for item in temp_colist if item not in self.tablecols[k]]
                        with engine.connect() as con:
                            for col in col_to_add:
                                self.tablecols[k].append(col)
                                query = f"ALTER TABLE \"{k}\" ADD COLUMN \"{col}\" TEXT;"
                                q_result = con.execute(text(query))
                                con.commit()
                                time.sleep(1)
                    df.to_sql(k, engine, index=False, if_exists='append')
                    if set_pkey_once is False:
                        try:
                            with engine.connect() as con:
                                query = f"ALTER TABLE \"{k}\" ADD PRIMARY KEY (id);"
                                q_result = con.execute(text(query))
                                logging.debug(q_result)
                                con.commit()
                                return_val = True
                        except exc.SQLAlchemyError as ex:
                            # df.to_sql() is setting primary key for Few tables. So 
                            # with this exception, it is ok to continue.
                            logging.warning("Unable to set primary key constraint: %s", str(ex))
            except exc.SQLAlchemyError as ex:
                logging.error("Error inserting records to database: %s", str(ex))
                return_val = False
            set_pkey_once = True
        logging.info("All records stored in database.")
        return return_val