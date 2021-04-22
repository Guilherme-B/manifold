# Project Manifold
## Overview

Manifold is a plug-and-play end-to-end real estate asset tracker. In other words, it encompasses the entire process of tracking real estate listings in a variety of sources (e.g. ERA, REMAX), from website scraping to the ETL process, culminating in a Data Warehouse (Kimball). Typically, Real Estate market studies are expensive or performed by external platforms. Manifold's goal is to provide everyone with the capability to track their local Real Estate market with little effort, and virtually no expenses.

The project is built on top of different languages and platforms:
* Scrapers
  * [GoLang] - Go is a statically typed, compiled programming language designed at Google by Robert Griesemer, Rob Pike, and Ken Thompson. Go is syntactically similar to C, but with memory safety, garbage collection, structural typing, and CSP-style concurrency making it a performant candidate for web scraping
  * [Python] (deprecated)
* ETL
  *  [Apache Airflow] - Apache Airflow is an open-source workflow management platform
  *  [Apache Spark] - Apache Spark is an open-source unified analytics engine for large-scale data processing
  *  [AWS EMR] - Amazon Elastic MapReduce (EMR) is a tool for big data processing and analysis on Apache Spark, Apache Hive, Apache HBase, Apache Flink, Apache Hudi, and Presto
  *  [AWS Redshift] - Amazon Redshift is a data warehouse product that forms part of the larger cloud-computing platform Amazon Web Services
  *  [AWS S3] - Amazon Simple Storage Service (Amazon S3) is an object storage service that offers industry-leading scalability, data availability, security, and performance
  

![DAG](https://github.com/Guilherme-B/manifold/blob/main/images/dag/graph.png)

## Features

- Fast web scrapers ([GoLang Colly])
- Scalable ETL ([Apache Spark], [AWS EMR], [AWS S3], [AWS Redshift])
- Pre-built ETL DAG ([Apache Airflow])
- Backfill - only the resources for the current timestep are consumed (*default* weekly)
- Extensible
- Cheap - resources are allocated and deallocated on a need basis

## Structure

The project is divided into two main categories: web scrapers (crawlers) and ETL resources.

| Folder | Category | Description |
| ------ | ------ |  ------ |
| crawler | Crawlers | Go and Python (deprecated) web scrapers |
| dags | ETL | Apache Airflow DAGs |
| plugins | ETL | Apache Airflow extensions (Operators, Sensors, utilities) |
| scripts | ETL | Python and Bash scripts to be stored and executed by third parties (Apache Spark, AWS EMR) |
| images | miscellaneous | Documentation support images |


## ETL Process

The ETL process consists of five interlinked and linear steps:
1. Web scraping ([GoLang] by default)
2. Intermmediate layer - source consumption and staging preparation in Parquet format ([Apache Spark], [AWS EMR], [AWS S3])
3. Staging layer- Parquet to AWS Redshift ([AWS Redshift], [AWS S3])
4. Presentation layer - presentation layer creation with Slowly Changing Dimensions (type 2)

Note: The scrapers were not included in the DAG given different users will aim at executing them in different manners (locally, docker, Kubernetes) hence, a dummy operator replaces the scrapers by default.

### Intermmediate layer
![EMR Create](https://github.com/Guilherme-B/manifold/blob/main/images/dag/emr_create.png)

Launches an EMR (*default* cluster) instance, which installs the required packages using [scripts/bootstrap_install_python_modules.sh], and runs the [Apache Spark] ETL process via [scripts/el_to_parquet.py]. 

[scripts/el_to_parquet.py] in in charge of the following tasks:
- locating the S3 bucket for the referenced timestep (*default* weekly)
- consuming the identified JSON sources
- cleanup and standardization
- staging layer creation (Parquet files)
- save Parquet files to an S3 bucket

### Staging layer

The staging layer is in charge of consuming the [AWS S3] Parquet files into [AWS Redshift]. The process is handled by the S3 to Redshift Operator.

The DAG is then comprised of two steps:

1- Assert the existence of the proper schemas, and create them if not present
![Schema Create](https://github.com/Guilherme-B/manifold/blob/main/images/dag/staging_schema.png)
2- Copy the Parquet files into the appropriate dimensional object
![Staging Copy](https://github.com/Guilherme-B/manifold/blob/main/images/dag/staging_tables.png)

### Presentation layer

The presentation or consumption layer holds the final updated dimensional model. Roughly speaking, the layer's responsibilities are:
1- Calculate dimension Deltas
2- Upsert dimension data according to the computed Delta
![Presentation Dimension](https://github.com/Guilherme-B/manifold/blob/main/images/dag/presentation_dimensions.png)
3- Append to the fact tables
![Presentation Fact](https://github.com/Guilherme-B/manifold/blob/main/images/dag/presentation_facts.png)

Operations 1 and 2 are the responsibility of the [Dimension Operator], whereas the Fact appending is handled via Postgres Operator.

## Custom Operators

Two operators were introduced to facilitate and replicate Airflow functions:
- [Dimension Operator]
- [S3 to Redshift Operator]

### Dimension Operator

The [Dimension Operator] computes the Deltas between a *base_table* and *target_table*, by comparing the *(SHA256) hash* between the two tables (typically, staging vs presentation), using *match_columns* as the business keys, and updates the *target_table* by implementing a Slowly Changing Dimension of type 2.

| Argument | Type | Description |
| ------ | ------ |  ------ |
| postgres_conn_id | str | The Airflow Redshift connection ID |
| target_table | str | The table to be updated (typically, presentation) |
| base_table | str | The base table to use as current value (typically, staging) |
| match_columns | List[str] | The list of columns representing business keys |
| database_name | str | The target database (*default dev*) |

### S3 to Redshift Operator

The [S3 to Redshift Operator] is responsible for taking a set of Parquet files stored in an [AWS S3] bucket and pushing them to the defined [AWS Redshift] cluster.


| Argument | Type | Description |
| ------ | ------ |  ------ |
| redshift_conn_id | str | The Airflow Redshift connection ID |
| redshift_credentials_id | str | The Airflow connection containing the Redshift credentials |
| s3_path | str | The base [AWS S3] URL holding the Parquet files |
| s3_bucket_template | str | The template S3 Bucket template (for backfilling, *default "/{year}/{month}/{week}/"* |
| destination_name | str | The [AWS Redshift] destination table name |
| source_name | str | The Parquet file name |
| role_name | str | The [AWS Redshift] role name |
| region_name | str | The [AWS Redshift] cluster region |

### Sources

Manifold comes with two out-of-the-box scrapers: one developed in GoLang, two developed in Python (deprecated). However, given the small number of local listings per website (around 10.000), Manifold has been tested on 50 million records of weekly data, in addition to the weekly scraped listings. 

External data sources:
* [Argentina Data]
* [Colombia Data]
* [Ecuador Data]
* [Uruguay Data]
* [Peru Data]
* [Brazil Data]
* [Spain (Madrid) Data]
* [Mexico Data]

The data was then aggregated, and a 10-week weekly evolution was simulated: in each week, a sample of 80% of the dataset was retrieved, simulating new and removed assets; in addition to price fluctuations.

The final dataset contained around 50GB of data.

## Installation

Manifold is a plug-and-play project, which is fully working from the moment [Apache Airflow] is started. Nonetheless, there are configurations that need to be set and connections to be created in order to drive the DAG and make sure it's pointing in the right direction, with the appropriate permissions. 

### Configuration
#### Variables
| Variable Name | Description |
| ------ |  ------ |
| manifold_s3_path |  The [AWS S3] base bucket name |
| manifold_s3_template |  The template S3 Bucket template (for backfilling, *default "/{year}/{month}/{week}/"* |

#### Connections
| Connection Name | Type | Description | Extra |
| ------ | ------ |  ------ |   ------ |
| aws_credentials | Amazon Web Services | The AWS  ([AWS S3]) login (Access Key) and password (Secret Access Key)  | {"region_name": "your_region_name"} | 
| emr_credentials | Amazon Web Services | The [AWS EMR] login (Access Key) and password (Secret Access Key) | |
| redshift_conn | Postgres | The [AWS Redshift] connection details, host, schema, login, password, port | |

### Docker

Apache Spark can be started by using docker-compose on the provided image, which takes care of the entire process including the first-time setup:
```sh
cd manifold
docker-compose up -d
```

## License

GNU General Public License v3.0


[//]: # (Reference links)

   [GoLang]: <https://golang.org/>
   [Python]: <https://www.python.org/>
   [Apache Airflow]: <https://airflow.apache.org/>
   [Apache Spark]: <https://spark.apache.org/>
   [AWS EMR]: <https://aws.amazon.com/emr/>
   [AWS Redshift]: <https://aws.amazon.com/redshift/>
   [AWS S3]: <https://aws.amazon.com/s3/>
   [GoLang Colly]: <http://go-colly.org/>
   [crawlers]: <https://github.com/Guilherme-B/manifold/tree/main/crawler>
   [scripts/bootstrap_install_python_modules.sh]: <https://github.com/Guilherme-B/manifold/blob/main/scripts/bootstrap_install_python_modules.sh>
   [scripts/el_to_parquet.py]: <https://github.com/Guilherme-B/manifold/blob/main/scripts/el_to_parquet.py>
   [S3 to Redshift Operator]: <https://github.com/Guilherme-B/manifold/blob/main/plugins/operators/s3toredshift_operator.py>
   [Dimension Operator]: <https://github.com/Guilherme-B/manifold/blob/main/plugins/operators/dimension_operator.py>
   
   [Argentina Data]: <https://storage.googleapis.com/properati-data-public/ar_properties.csv.gz>
   [Colombia Data]: <https://storage.googleapis.com/properati-data-public/co_properties.csv.gz>
   [Ecuador Data]: <https://storage.googleapis.com/properati-data-public/ec_properties.csv.gz>
   [Uruguay Data]: <https://storage.googleapis.com/properati-data-public/uy_properties.csv.gz>
   [Peru Data]: <https://storage.googleapis.com/properati-data-public/pe_properties.csv.gz>
   [Brazil Data]: <https://data.world/properati/real-estate-listings-brazil>
   [Spain (Madrid) Data]: <https://www.kaggle.com/mirbektoktogaraev/madrid-real-estate-market>
   [Mexico Data]: <https://data.world/properati/real-estate-listings-mexico>