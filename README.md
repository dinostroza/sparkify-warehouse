# Sparkify Warehouse

This project is a sample Warehouse for Sparkify, a music streaming startup. This can be used to find insights about songs trends from user activity.

### Project Structure

The project uses AWS Redshift as Warehouse technology database and S3 for storage raw dataset. The structure and population of database is managed by python scripts.

- `dwh.cfg`: config file for setting main connection parameters of AWS Redshift and S3.
- `sql_queries.py`: contains all queries for creation and etl scripts.
- `create_database.py`: python script file for creation database tables.
- `etl.py`: python script file for populate database tables.

### Python dependecies

This project requires Python 3.x and the following Python libraries installed:

- `psycopg2` PostgreSQL database adapter 
- `boto3` AWS SDK library for infrastructure as code

To install them you can run the next command:

```bash
pip install -r requirements.txt
```

Note: You should use a virtual environment 

### AWS Configuration

To run this project first you need to create a redshift cluster. Then edit the main parameters of your AWS services in the `fdwh.cfg` file:

- Redshift Cluster parameters:
    - `HOST`: the Redshift database endpoint to connect .
    - `DB_NAME`: the name of redshift database.
    - `DB_PORT`: the port of database connection.
    - `DB_USER` and `DB_PASSWORD`: user and password credentials to access database.

- IAM parameters:
    - `ARN` The role arn to be used by redshift to access S3 buckets.

- S3 parameters (must include **s3://**):
    - `LOG_DATA`: Path or prefix to log events bucket of Spartify.
    - `LOG_JSONPATH`: Path to log format json data file of events to read the data using COPY command.
    - `SONG_DATA`: Path or prefix to songs data of Spartify.

### Create Database

To create the database you only need to run `create_tables.py`. 

```bash
python create_tables.py
```

this creates the staging, fact and dimension tables in database.

### Populate Database

Finally, to populate the tables with S3 dataset run the `etl.py` script:

```bash
python etl_tables.py
```
