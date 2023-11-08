import yaml
import geopandas as gpd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
import logging
from .. import get_db_connection

log = logging.getLogger(__name__)
class AddData:
    def __init__(self, path,config_file="credentials.yaml"):
        """Define a class to add data to the database
        parameters:
        path: dict - Contains the bucket and key of the file to be processed
        config_file: str - Path to the yaml credentials file (database connection info)"""
        self.path = path
        self.config_file = config_file
        self._load_config(self.config_file)
        self.s3_path = self._get_s3_path()
        self.data = self._read_data()
        
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
        
    def _read_data(self)->gpd.GeoDataFrame:
        """Read the data from the s3 bucket and return a geopandas dataframe"""
        log.info(f"Loading {self.s3_path}")
        gdf = gpd.read_file(self.s3_path)
        if gdf.crs is None or gdf.crs.to_epsg() != 4326:
            raise ValueError("Shapefile does not have CRS EPSG:4326")
        return gdf
    def _read_source_data(self)->gpd.GeoDataFrame:
        """ Read the Structure inventory data from the database and return a geopandas dataframe"""
        sql = f"SELECT * FROM {self.tables[2]['name']}"
        df = gpd.read_postgis(sql, self.engine,geom_col='geometry')
        return df
    
    def _get_s3_path(self):
        """Return the s3 path of the file to be processed"""
        return f"s3://{self.path['Bucket']}/{self.path['Key']}"
    
    def get_data(self):
        "Method to view the raw data"
        return self.data
    
    def process_data(self)->gpd.GeoDataFrame:
        """Process the data and return a geopandas dataframe with the processed data as Geodataframe"""
        columns = [
            'gdb_geomattr_data',
            'shape',
            'storm_id',
            'fd_id',
            'x',
            'y',
            'depth',
            'damage_cat',
            'occupancy',
            'structure',
            'content_da',
            'total_damage',
            'pop2amu65',
            'pop2amo65',
            'pop2pmu65',
            'pop2pmo65',
            'path_aws',
            's_dam_per',
            'c_dam_per',
            'occupancy_str',
            'damage_cat_str',
            'found_ht',
            'depth_above_ff']
        self.data.rename(columns={'geometry':'shape','damage cat':'damage_cat','content da':'content_da'}, inplace=True)
        source_data = self._read_source_data()
        source_data['fd_id'] = source_data['fd_id'].astype(int)
        source_data=source_data[['fd_id','found_ht']]
        self.data['fd_id'] = self.data['fd_id'].astype(int)
        self.data = gpd.GeoDataFrame(self.data.merge(source_data,on='fd_id',how='left',suffixes=(None, '_y')))
        self.data['depth_above_ff'] = self.data['depth'] - self.data['found_ht']
        self.data['storm_id'] = self.insert_storm_name()
        self.data['gdb_geomattr_data'] = np.nan
        self.data['total_damage'] = self.data['content_da'] + self.data['structure']
        self.data['path_aws'] = self.s3_path
        self.data['occupancy_str'] = self.data['occupancy'].apply(self.extract_occupancy)
        self.data['damage_cat_str'] = self.data['damage_cat'].apply(self.extract_damage_category)
        self.data=self.data.loc[self.data['total_damage']>0]
        self.data.set_geometry('shape',inplace=True)
        self.data.to_crs(3857,inplace=True)
        return self.data[columns]
        
    def get_storm_name(self)->str:
        """Return the storm name from the s3 path"""
        filename = self.path['Key'].split("/")[-1]
        storm_name = filename.split("_")[:2]
        return "_".join(storm_name)
    
    def extract_occupancy(self,occ_type:str)->str:
        """Extract the occupancy type from the occ_type column"""
        occ_type_dict={
            'RES1':'Single Family',
            'RES2':'Mobile Home',
            'RES3A':'Duplex',
            'RES3B':'Multi-Family 3-4 Units',
            'RES3C':'Multi-Family 5-9 units',
            'RES3D':'Multi-Family 10-19 units',
            'RES3E':'Multi-Family 20-19 units',
            'RES3F':'Multi-Family 50+ units',
            'RES4':'Temporary Lodging',
            'RES5':'Institutional Dormitory',
            'RES6':'Nursing Home',
            'COM1':'Retail Trade',
            'COM2':'Wholesale Trade',
            'COM3':'Personal and Repair Service',
            'COM4':'Professional/Technical/Business Services',
            'COM5':'Banks/Financial Institutions',
            'COM6':'Hospital',
            'COM7':'Medical Office/Clinic',
            'COM8':'Entertainment & Recreation',
            'COM9':'Theaters',
            'IND1':'Heavy Industrial Factory',
            'IND2':'Light Industrial Factory',
            'IND3':'Food/Drug/Chemicals Factory',
            'IND4':'Metals/Minerals Processing Factory',
            'IND5':'High Technology Factory',
            'IND6':'Construction',
            'AGR':'Agriculture',
            'REL':'Church/Membership Organization 1',
            'GOV1':'General Service',
            'GOV2':'Emergency Response',
            'ED1':'Schools/Libraries',
            'ED2':'College/University'
        }
        acronyms = occ_type.split("-")
        occupancy = ""
        for acronym in acronyms:
            if acronym in occ_type_dict.keys():
                occupancy += f"{occ_type_dict[acronym]} "
            elif acronym.endswith("SNB"):
                if int(acronym[0])==1:
                    occupancy += f"{acronym[0]} Story no basement"
                else:
                    occupancy += f"{acronym[0]} Stories no basement"
            elif acronym=="PIER":
                continue
            else:
                occupancy += f" {acronym}"
        
        return occupancy
    
    def extract_damage_category(self,damage_cat:str)->str:
        """Extract the damage category from the damage_cat column"""
        damage_cat_dict={
            'Res': 'Residential',
            'Com': 'Commercial',
            'Ind': 'Industrial',
            'Pub': 'Public',
        }
        if damage_cat in damage_cat_dict.keys():
            damage_category = f"{damage_cat_dict[damage_cat]}"
        else:
            damage_category = "Unknown"
        return damage_category
    
    def insert_storm_name(self)->int:
        """Insert the storm name into the database and return the storm_id"""
        sql_insert = f"INSERT INTO {self.tables[1]['name']} (storm) VALUES ('{self.get_storm_name()}')"
        sql_select=f"SELECT storm_id FROM {self.tables[1]['name']} WHERE storm='{self.get_storm_name()}'"
        cursor = self.connection.cursor()
        cursor.execute(sql_select)
        storm_id = cursor.fetchone()
        if storm_id:
            self.connection.commit()
            cursor.close()
            return int(storm_id[0])
        else: 
            cursor.execute(sql_insert)
            self.connection.commit()
            cursor.execute(sql_select)
            storm_id = cursor.fetchone()[0]
            cursor.close()
            return int(storm_id)
        
        
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
    def save_data(self,processed_data:gpd.GeoDataFrame):
        """Save the processed data into the database"""
        try:
            processed_data.to_postgis("result", self.engine,if_exists='append',schema=self.schema)  
            self.clean_storm_data()
            self.connection.commit()
            self.connection.close()
        except (Exception, psycopg2.DatabaseError) as error:
            log.info(error)
        finally:
            if self.connection is not None:
                self.connection.close()
    def execute(self)->bool:
        """Execute the pipeline"""
        try:
            log.info(f"Processing {self.s3_path}")
            processed_data = self.process_data()
            log.info(f"inserting {self.s3_path} into the database")
            self.save_data(processed_data)
            log.info(f"Finished processing {self.s3_path}")
            return True
        except Exception as e:
            log.error(f"Error processing {self.s3_path}")
            log.error(e)
            return False        
        