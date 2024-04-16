import yaml
import geopandas as gpd
import numpy as np
import psycopg2
from sqlalchemy import create_engine, text
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
        self.regions_id= self.__get_regions(self.data)
        storm_data=self.__get_storm_name()
        if storm_data is None:
            print("Tropical and Nontropical storms are not supported")
            return None
        else: 
            self.storm_name = storm_data['storm_name']
            self.storm_event_type = storm_data['event_type']
        self.storm_id=self.__insert_event()
        
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
        log.info(f"Finished loading {self.s3_path}")
        return gdf
    def _read_source_data(self)->gpd.GeoDataFrame:
        """ Read the Structure inventory data from the database and return a geopandas dataframe"""
        with self.engine.connect() as conn:
            sql = text(f"SELECT * FROM {self.tables[2]['name']}")
            df = gpd.read_postgis(sql, conn,geom_col='shape')
        return df
    
    def _get_s3_path(self):
        """Return the s3 path of the file to be processed"""
        return f"s3://{self.path['Bucket']}/{self.path['Key']}"
    
    def __get_regions(self,result_data):
        """Return the region id"""
        result_data.to_crs(3857,inplace=True)
        sql = text(f"SELECT * FROM {self.tables[3]['name']}")
        with self.engine.connect() as conn:
            regions = gpd.read_postgis(sql, conn,geom_col='shape')
            region_ids = gpd.overlay(result_data, regions, how='intersection')['region_watershed'].unique()
        log.info(f"Region ids: {region_ids}")
        return region_ids
    
    def get_regions(self):
        """Return the region id"""
        return self.regions_id
    
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
        self.data['content_da'] = self.data['content_da'].round(-3)
        self.data['structure'] = self.data['structure'].round(-3)
        self.data = gpd.GeoDataFrame(self.data.merge(source_data,on='fd_id',how='left',suffixes=(None, '_y')))
        self.data['depth_above_ff'] = self.data['depth'] - self.data['found_ht']
        self.data['storm_id'] = self.storm_id
        self.data['gdb_geomattr_data'] = np.nan
        self.data['total_damage'] = self.data['content_da'] + self.data['structure']
        self.data['path_aws'] = self.s3_path
        self.data['occupancy_str'] = self.data['occupancy'].apply(self.extract_occupancy)
        self.data['damage_cat_str'] = self.data['damage_cat'].apply(self.extract_damage_category)
        self.data=self.data.loc[self.data['total_damage']>0]
        self.data.set_geometry('shape',inplace=True)
        self.data.to_crs(3857,inplace=True)
        return self.data[columns]
        
    def __get_storm_name(self)->str:
        """Return the storm name from the s3 path"""
        filename = self.path['Key'].split("/")[-1]
        name = filename.replace(" ","").split("_")
        log.info(f"Storm name: {name}")
        log.info(f"Storm name length: {len(name)}")
        if len(name)>=12:
            storm_name = name[2] #name[7]
            #frequency = name[2]
            event_type= 1
        else:
            if name[1] == "CMB":
                storm_name = name[3]  ##f"{name[1]}, {name[2]}"
                #frequency = name[3]
                event_type = 3
            elif name[1] in ["nTC","TC"]:
                return None
            else:
                storm_name = name[2]
                #frequency = "N.A."
                event_type = 2
        return {
            "storm_name":storm_name,
            "event_type":event_type
        }
    
    def get_storm_name(self):
        """Return the storm name"""
        return self.storm_name
    
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
    
    def _check_and_insert_data(self,sql_select:str,sql_insert:str)->int:
        """Check if the data exists in the databaseor insert the data"""
        cursor = self.connection.cursor()
        cursor.execute(sql_select)
        row_id = cursor.fetchone()
        if row_id:
            self.connection.commit()
            cursor.close()
            row_id = int(row_id[0])
        else:
            cursor.execute(sql_insert)
            self.connection.commit()
            cursor.execute(sql_select)
            row_id_val = cursor.fetchone()[0]
            cursor.close()
            row_id = int(row_id_val)
        return row_id
    
    def __insert_event(self)->int:
        """Insert the storm name into the database and return the storm_id"""
        #sql_insert_frequency = f"INSERT INTO {self.tables[4]['name']} (event_type,frequency) VALUES ({self.storm_event_type},'{self.storm_frequency}')"
        #sql_select_frequency = f"SELECT id FROM {self.tables[4]['name']} WHERE event_type={self.storm_event_type} and frequency='{self.storm_frequency}'"
        ## Check if frequency and event type exists
        #frequency_id = self._check_and_insert_data(sql_select_frequency,sql_insert_frequency)
        sql_insert_storm = f"INSERT INTO {self.tables[1]['name']} (storm,event_type) VALUES ('{self.storm_name}',{self.storm_event_type})"
        sql_select_storm=f"SELECT storm_id FROM {self.tables[1]['name']} WHERE storm='{self.storm_name}' AND event_type={self.storm_event_type}"
        storm_id = self._check_and_insert_data(sql_select_storm,sql_insert_storm)
        return storm_id
        
    
    def get_storm_id (self)->int:
        """Return the storm_id"""
        return self.storm_id
        
    def _delete_isolate_data(self, sql_select:str,table:str, field:str):
        """Delete data that is not connected to the results table"""
        cursor = self.connection.cursor()
        cursor.execute(sql_select)
        ids = cursor.fetchall()
        if ids:
            for id in ids:
                sql_delete = f"DELETE FROM {table} WHERE {field}={id[0]}"
                cursor.execute(sql_delete)
            self.connection.commit()
            cursor.close()
        return True
    def clean_storm_data(self):
        """Delete storm names that are not related into the results table"""
        sql_select=f"SELECT storm_id FROM {self.tables[1]['name']} WHERE storm_id not in (SELECT DISTINCT storm_id FROM {self.tables[0]['name']})"
        self._delete_isolate_data(sql_select,self.tables[1]['name'],'storm_id')
        #sql_select_frequency=f"SELECT id FROM {self.tables[4]['name']} WHERE id not in (SELECT DISTINCT frequency_id FROM {self.tables[1]['name']})"
        #self._delete_isolate_data(sql_select_frequency,self.tables[4]['name'],'id')
        
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
        