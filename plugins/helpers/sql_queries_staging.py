from dataclasses import dataclass
from typing import Any, List, Dict, Generator, Tuple


@dataclass
class CopyConfig:
    destination_name: str
    source_name: str

create_staging_schema: str = '''
    create schema if not exists staging;
'''

broker_staging_create = '''
  DROP TABLE IF EXISTS staging.dim_broker;
  
  CREATE TABLE staging.dim_broker 
  (
      broker         varchar,
      hash           varchar
  )
'''

asset_staging_create = '''
  DROP TABLE IF EXISTS staging.dim_asset;
  
  CREATE TABLE staging.dim_asset
  (
        contract_number          varchar,
        country                 varchar,
        county                  varchar,
        parish                  varchar,
        title                   varchar(max),
        description             varchar(max),
        price                   float,
        property_type            varchar,
        bathrooms               float,
        bedrooms                float,
        area_net                 float,
        latitude                float,
        longitude               float,
        hash                    varchar
    )
'''

stock_staging_create = '''
  DROP TABLE IF EXISTS staging.fact_stock;
  
  CREATE TABLE staging.fact_stock (
        broker         varchar,
        contract_number varchar,
        country        varchar,
        county         varchar,
        parish         varchar,
        price          float,
        quantity       int,
        stock_date     varchar
    )
'''

geography_staging_create = '''
  DROP TABLE IF EXISTS staging.dim_geography;
  
  CREATE TABLE staging.dim_geography
  (
        country varchar,
        county varchar,
        parish varchar,
        hash varchar
  )

'''

# holds the Redshift presentation COPY statements
copy_query_definition: Dict[str, CopyConfig] = {
    'staging_dim_asset': CopyConfig(destination_name='staging.dim_asset', source_name='{bucket_name}asset_staging.parquet'),
    'staging_dim_geography': CopyConfig(destination_name='staging.dim_geography', source_name='{bucket_name}geography.parquet'),
    'staging_dim_broker': CopyConfig(destination_name='staging.dim_broker', source_name='{bucket_name}broker_staging.parquet'),
    'staging_fact_stock': CopyConfig(destination_name='staging.dim_broker', source_name='{bucket_name}asset_stock.parquet'),
}

# holds the Redshift presentation table creation definition
create_query_destinition: Dict[str, str] = {
    'staging_broker': broker_staging_create,
    'staging_asset': asset_staging_create,
    'staging_geography': geography_staging_create,
    'staging_stock': stock_staging_create
}
