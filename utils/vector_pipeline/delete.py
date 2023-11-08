import yaml
import geopandas as gpd
from sqlalchemy import create_engine, MetaData, Table
import logging
from .. import get_db_connection

log = logging.getLogger(__name__)

class DeleteData:
    def __init__(self, path,config_file="credentials.yaml"):
        """Define a class to add data to the database
        parameters:
        path: dict - Contains the bucket and key of the file to be processed
        config_file: str - Path to the yaml credentials file (database connection info)"""
        self.path = path
        self.config_file = config_file
        self._load_config(self.config_file)
        self.s3_path = self._get_s3_path()

        
    def _load_config(self,config_file):
        """Load the database credentials from the yaml file
        It sets the connection, engine, schema and tables attributes"""
        try:
            with open(config_file, 'r') as f:
                config_data = yaml.safe_load(f)
            db_host = config_data['database']['host']
            db_port = config_data['database']['port']
            db_name = config_data['database']['database']
            db_user = config_data['database']['user']
            db_password = config_data['database']['password']
            self.tables = [{"type":table['type'],"name":table['name']} for table in config_data['database']['tables']]
            self.connection = get_db_connection(db_name,db_user,db_password,db_host,db_port)
            self.engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}") 
            self.schema = db_user
            return True
        
        except FileNotFoundError:
            log.info("Credentials file not found")
            return None      
    
    def _get_s3_path(self):
        """Return the s3 path of the file to be processed"""
        return f"s3://{self.path['Bucket']}/{self.path['Key']}"
    
    def clean_storm_data(self):
        """Delete storm names that are not related into the results table"""
        sql_select=f"SELECT storm_id FROM {self.tables[1]['name']} WHERE storm_id not in (SELECT DISTINCT storm_id FROM {self.tables[0]['name']})"
        cursor = self.connection.cursor()
        cursor.execute(sql_select)
        storm_ids = cursor.fetchall()
        if storm_ids:
            for storm_id in storm_ids:
                sql_delete = f"DELETE FROM {self.tables[1]['name']} WHERE storm_id={storm_id[0]}"
                cursor.execute(sql_delete)
            self.connection.commit()
            cursor.close()
            
    def delete_data(self):
        """Save the processed data into the database"""
        metadata = MetaData()
        results_table = Table(self.tables[0]['name'], metadata, autoload_with=self.engine)
        stmt = results_table.delete().where(results_table.c.path_aws==self.s3_path)
        print(self.s3_path)
        print(stmt)
        with self.engine.begin() as connection:
            connection.execute(stmt)
            connection.commit()
                
    def execute(self)->bool:
        """Execute the pipeline"""
        try:
            log.info(f"Deleting {self.s3_path} from the database")
            self.delete_data()
            self.clean_storm_data()
            if self.connection is not None:
                self.connection.close()
            log.info(f"Finished processing {self.s3_path}")
            return True
        except Exception as e:
            log.error(f"Error processing {self.s3_path}")
            log.error(e)
            return False        
        