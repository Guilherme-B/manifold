from typing import Dict


create_presentation_schema: str = '''
    create schema if not exists presentation;
'''

create_presentation_dim_asset: str = '''
    create table presentation.dim_asset
    (
        asset_id		      bigint identity(0, 1) primary key, 
        contract_number       varchar not null,
        country               varchar,
        county                varchar,
        parish                varchar,
        title                 varchar(max),
        description           varchar(max),
        price                 float  not null,
        property_type         varchar,
        bathrooms             float,
        bedrooms              float,
        area_net              float,
        latitude              float,
        longitude             float,
        hash                  varchar not null,
        record_start_date 	  datetime default getdate(),
        record_end_date		  datetime default null
    )
'''

create_presentation_dim_broker: str = '''
    create table if not exists presentation.dim_broker
    (
        broker_id bigint        identity(0, 1) primary key,
        broker                  varchar,
        hash                    varchar not null,
        record_start_date 		datetime default getdate(),
        record_end_date			datetime default null
    )
'''

create_presentation_dim_geography: str = '''
    create table if not exists presentation.dim_geography
    (
        geography_id            bigint identity(0, 1) primary key,
        country                 varchar,
        county                  varchar,
        parish                  varchar,
        hash                    varchar not null,
        record_start_date 		datetime default getdate(),
        record_end_date			datetime default null
    )
'''

create_presentation_dim_date_view: str = '''
    create or replace view presentation.v_dim_date as
    (
    select
            to_char(datum, 'YYYYMMDD')::INTEGER								AS date_id , 
            to_char(datum, 'YYYY-MM-DD')									AS full_date,
            cast(extract(YEAR FROM datum) AS integer)                    	AS year_number,
            cast(extract(WEEK FROM datum) AS integer)                    	AS week_iso_number,
            cast(extract(DOY FROM datum) AS integer)                     	AS day_number,
            cast(to_char(datum, 'Q') AS integer)                         	AS quarter_number,
            cast(extract(MONTH FROM datum) AS integer)                   	AS month_number,
            to_char(datum, 'Month')                                       	AS month_name,
            cast(to_char(datum, 'D') AS integer)                         	AS weekday_number,
            to_char(datum, 'Day')                                         	AS day_name,
            case when to_char(datum, 'D') IN ('1', '7')
            then 0
            else 1 end                                                    	AS is_weekday,
            case when
            extract(DAY FROM (datum + (1 - extract(DAY FROM datum)) :: INTEGER +
                                INTERVAL '1' MONTH) :: DATE -
                            INTERVAL '1' DAY) = extract(DAY FROM datum)
            then 1
            ekse 0 end                                                    	AS is_last_of_month
    from
        -- Generate days for 30 years starting from 2018
        (
            select
                '2018-01-01' :: DATE + generate_series AS datum,
                generate_series                        AS seq
            from 
                generate_series(0, 30 * 365, 1)
        ) DQ
        order by 1
    )
'''

create_presentation_fact_stock: str = '''
    CREATE TABLE presentation.fact_stock 
    (
        id 			  		bigint identity(0, 1),
        broker_id        	bigint references presentation.dim_broker (id),
        asset_id 			bigint references presentation.dim_asset (id),
        geography_id        bigint references presentation.dim_geography (id),
        price          		float,
        quantity      		int,
        date_id     		bigint
    )
'''


populate_presentation_fact_stock: str = '''
    insert into presentation.fact_stock
        (
        broker_id
        ,asset_id
        ,geography_id
        ,date_id
        ,price
        ,quantity
        )
    select
        dim_broker.id broker_id
        ,dim_asset.id asset_id
        ,dim_geography.id geography_id
        ,to_char(fact_stock.stock_date::date, 'YYYYMMDD')::INTEGER date_id
        ,fact_stock.price
        ,fact_stock.quantity
    from
        staging.fact_stock
    left join
        presentation.dim_asset
    on
        fact_stock.contract_number =  dim_asset.contract_number
    left join
        presentation.dim_broker
    on
        fact_stock.broker = dim_broker.broker
    left join
        presentation.dim_geography
    on
        fact_stock.country = dim_geography.country
        and
        fact_stock.county = dim_geography.county
        and
        fact_stock.parish = dim_geography.parish
'''

dimension_definitions: Dict[str, Dict[str, str]] = {
    'presentation_dim_broker': {
        'target_table': 'dim_broker',
        'base_table': 'dim_broker',
        'match_columns': ['broker']
    },
    'presentation_dim_asset': {
        'target_table': 'dim_asset',
        'base_table': 'dim_asset',
        'match_columns': ['contract_number']
    },
    'presentation_dim_geography': {
        'target_table': 'dim_geography',
        'base_table': 'dim_geography',
        'match_columns': ['country','county', 'parish']
    },
}

fact_definitions: Dict[str,str] = {
    'presentation_fact_stock': populate_presentation_fact_stock,
}