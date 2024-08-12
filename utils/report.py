from sqlalchemy import create_engine, MetaData, Table, and_, text
import pandas as pd
import boto3
from io import StringIO
import logging
import yaml
from . import get_db_connection
from sqlalchemy import insert, select, delete

log = logging.getLogger(__name__)


class Report:
    def __init__(
        self,
        region_id: int,
        storm_id: int,
        config_file="credentials.yaml",
    ):
        """Define a class to add data to the database
        parameters:
        region_id: int - Region id to generate or delete the reports
        storm_id: int - Storm id to generate or delete the reports
        config_file: str - Path to the yaml credentials file (database connection info)
        """
        self.region_id = region_id
        self.storm_id = storm_id
        self.config_file = config_file

        self._load_config(self.config_file)
        self.s3_resource = boto3.resource("s3")

    def _load_config(self, config_file):
        """Load the database credentials from the yaml file
        It sets the connection, engine, schema and tables attributes"""
        try:
            with open(config_file, "r") as f:
                config_data = yaml.safe_load(f)
            db_host = config_data["database"]["host"]
            db_port = config_data["database"]["port"]
            db_name = config_data["database"]["database"]
            db_user = config_data["database"]["user"]
            db_password = config_data["database"]["password"]
            self.tables = [
                {"type": report["type"], "name": report["name"]}
                for report in config_data["database"]["report"]
            ]
            self.connection = get_db_connection(
                db_name, db_user, db_password, db_host, db_port
            )
            self.engine = create_engine(
                f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            )
            self.schema = db_user
            self.bucket_name = config_data["bucket"]["public_name"]
            self.bucket_region = config_data["bucket"]["region"]
            return True

        except FileNotFoundError:
            log.info("Credentials file not found")
            return None

    def __insert_to_table(self, boundary_type: str):
        """Insert the report path into the database"""

        log.info(
            f"Inserting report data for region {self.region_id} and storm {self.storm_id}"
        )
        metadata = MetaData()
        report_table = Table(
            self.tables[2]["name"], metadata, autoload_with=self.engine
        )
        select_stmt = select(report_table).where(
            (report_table.c.region_id == int(self.region_id))
            & (report_table.c.storm_id == int(self.storm_id))
            & (report_table.c.boundary_type == boundary_type)
        )
        with self.engine.connect() as conn:
            existing_row = conn.execute(select_stmt).fetchone()
            if existing_row:
                log.info(f"Report {existing_row} already exists")
                return existing_row
            else:
                insert_stmt = insert(report_table).values(
                    region_id=int(self.region_id),
                    storm_id=int(self.storm_id),
                    boundary_type=boundary_type,
                    aws_path=f"https://{self.bucket_name}.s3.{self.bucket_region}.amazonaws.com/consequence_reports/download.html?Storm_{self.storm_id}/Region_{self.region_id}/S{self.storm_id}_R{self.region_id}_B_{boundary_type}.csv",
                )
                result = conn.execute(insert_stmt)
                conn.commit()
                log.info(f"Report {result.inserted_primary_key} inserted")
                return result.inserted_primary_key

    def __delete_to_table(self):
        """delete the report path into the database"""

        log.info(
            f"Deleting report data for region {self.region_id} and storm {self.storm_id} from table"
        )
        metadata = MetaData()
        report_table = Table(
            self.tables[2]["name"], metadata, autoload_with=self.engine
        )
        delete_stmt = delete(report_table).where(
            and_(
                report_table.c.region_id == int(self.region_id),
                report_table.c.storm_id == int(self.storm_id),
            )
        )
        with self.engine.connect() as conn:
            conn.execute(delete_stmt)
            conn.commit()
        log.info(
            f"Reports for storm {self.storm_id} and region {self.region_id} deleted"
        )

    def generate(self):
        """Generate the report"""
        log.info(
            f"Generating report for region {self.region_id} and storm {self.storm_id}"
        )
        aggregate_at_risk_table = self.tables[0]["name"]
        results_table = self.tables[1]["name"]
        with self.engine.connect() as conn:
            at_risk_sql = text(
                f"select * from {aggregate_at_risk_table} where region_id={self.region_id}"
            )
            at_risk_data = pd.read_sql_query(
                at_risk_sql,
                con=conn,
            )
            sql_results = text(
                f"select * from {results_table} where region_id={self.region_id} and storm_id={self.storm_id}"
            )
            results_data = pd.read_sql_query(
                sql_results,
                con=conn,
            )
        category_dict = {
            "Com": "Commercial",
            "Ind": "Industrial",
            "Pub": "Public",
            "Res": "Residential",
        }
        at_risk_data["category"] = at_risk_data["category"].map(category_dict)
        merged_data = results_data.merge(
            at_risk_data,
            how="left",
            left_on=[
                "name20",
                "geoid20",
                "boundary_type",
                "boundary_name",
                "damage_cat_str",
            ],
            right_on=[
                "us_block_name",
                "block_code",
                "boundary_type",
                "boundary_name",
                "category",
            ],
        )
        report_cols = [
            "block_code",
            "us_block_name",
            "boundary_type",
            "boundary_name",
            "category",
            "si_affected",
            "si_at_risk",
            "total_damage",
            "content_damage",
            "structure_damage",
            "total_value_at_risk",
            "content_value_at_risk",
            "structure_value_at_risk",
        ]
        merged_data = merged_data[report_cols]
        renamed_cols = {
            "block_code": "Block code",
            "us_block_name": "Block name",
            "category": "Category",
            "si_affected": "Affected structures",
            "si_at_risk": "Structures at risk",
            "total_damage": "Total damage cost",
            "content_damage": "Content damage cost",
            "structure_damage": "Structure damage cost",
            "total_value_at_risk": "Total value at risk",
            "content_value_at_risk": "Content value at risk",
            "structure_value_at_risk": "Structure value at risk",
        }
        for bt in merged_data.boundary_type.unique():
            result_df = pd.DataFrame(columns=merged_data.columns)
            csv = merged_data.loc[merged_data["boundary_type"] == bt].copy()
            for block in csv.block_code.unique():
                block_df = csv[csv["block_code"] == block].copy()
                total_row = (
                    block_df.groupby("block_code")
                    .agg(
                        {
                            "us_block_name": "first",
                            "boundary_type": "first",
                            "boundary_name": "first",
                            "si_affected": "sum",
                            "si_at_risk": "sum",
                            "total_damage": "sum",
                            "content_damage": "sum",
                            "structure_damage": "sum",
                            "total_value_at_risk": "sum",
                            "content_value_at_risk": "sum",
                            "structure_value_at_risk": "sum",
                        }
                    )
                    .reset_index()
                )
                total_row["block_code"] = block
                total_row["category"] = "Total"
                result_df = pd.concat(
                    [result_df, block_df, total_row], ignore_index=True
                )

            result_df.drop(columns=["boundary_type"], inplace=True)
            renamed_cols["boundary_name"] = bt
            result_df.rename(columns=renamed_cols, inplace=True)
            filename = f"S{self.storm_id}_R{self.region_id}_B_{bt}.csv"
            csv_buffer = StringIO()
            result_df.to_csv(csv_buffer, index=False)
            log.info(f"Uploading report: {filename}")

            self.s3_resource.Object(
                self.bucket_name,
                f"consequence_reports/Storm_{self.storm_id}/Region_{self.region_id}/{filename}",
            ).put(Body=csv_buffer.getvalue())
            log.info(f"Report generated: {filename}")
            self.__insert_to_table(bt)
        return True

    def delete(self):
        """Delete the report"""
        log.info(
            f"Deleting report for region {self.region_id} and storm {self.storm_id}"
        )
        bucket = self.s3_resource.Bucket(self.bucket_name)
        to_delete = bucket.objects.filter(
            Prefix=f"consequence_reports/Storm_{self.storm_id}/Region_{self.region_id}/"
        )
        to_delete.delete()
        self.__delete_to_table()
        return True
