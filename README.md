# flake news
A batch loading tool for migrating data from an RDBMS to Snowflake

***Currently only supporting MSSQL Server***


----
## tl;dr
0. Setup & install, then export the environment variables

1. Edit the `table_config.yml` file with the database, schema, tables you want

2. Generate the `table_rules.json` file and the `table_ddl.sql` file
```sh
python -m flakenews -c table_config.yml
```

3. Create all the destination tables on Snowflake - hint: `table_ddl.sql` has what you need

4. Run the batch-load one-off process to load all the tables
```sh
python -m flakenews -r table_rules.json
```
5. Go push some Jira tickets to the done column.
----

## When do you need flake news ?
Very rarely.

* If you have a database that is already bulk-loaded, for example a datamart, and you want to temporarily transfer data from there to Snowflake.
* If that data mart experiences minimally logged operations like `TRUNCATE` that preclude the use of CDC
* If there are infrequent burst loads that would be overwhelming and expensive on tools like Fivetran


## When flake news is wrong
Frequently.

* If you have an application database that experiences small transactional changes
  * you should be using some form of _change data capture_ along with a friendly SaaS tool like Fivetran, Stitchdata, Hevo and similar, or use something like Debezium.
  * For MSSQL Server that means using CDC if possible, or CT if not.
  * For PostgreSQL that means using replication slots
  * For MySQL that means using the binlog

----
## Local Development

### Setup & Requirements

1. Python 3.8+
2. Create a python virtual environment: `python -m venv .venv`
3. Install required python packages: `pip install -r requirements.txt`
4. On mac you also need to: `brew install freetds` which is required by pymssql

### 1. Environment variables
_Export your environment variables_

Note: On windows you can run a line for each of these environment variables.
e.g. like:
```powershell
[System.Environment]::SetEnvironmentVariable('FN_SQL_SERVER','<name of server here>','User')
...
```

*MSSQL*
> Use forward slash `/` for the following, *not* a backslash. 
* FN_SQL_SERVER - either a `dbhostname/myinstance'` or `servername`
* FN_SQL_USER - either `companydomain/username` a `username`
* FN_SQL_PASSWORD 
* FN_SQL_PORT - optional, default is 1433

*Snowflake*

> Snowflake database / schema / tablenames need to be uppercase because the SDK will quote them.
* FN_SF_ACCOUNT - e.g. mh85760.ap-southeast-2 region / privatelink segment may be required
* FN_SF_USER - login name
* FN_SF_PASSWORD
* FN_SF_AUTHENTICATOR - for OKTA use `externalbrowser`
* FN_SF_ROLE
* FN_SF_WAREHOUSE
* FN_SF_DATABASE
* FN_SF_SCHEMA


### Create a table_config.yml file
_fake news can only work with a single MSSQL Server, but can target multiple databases and tables within that server_
* To begin you need to create a `table_config.yml` file similar to [table_config_example.yml](./table_config_example.yml) following, replacing the fields with your real information:
```yaml
version: 2

databases:
  - name: demodata
    schemas:
      - name: dbo
        tables:
          - name: bulk_data
         #- name: another_table_in_same_schema
     #- name: another_schema_in_same_database
  - name: alternate
    schemas:
      - name: dbo
        tables:
          - name: hey

```

### Generate table rules and ddl
With the environment variables ready and the table_config.yml file filled out you can generate: 
1. `table_ddl.sql` file - all of the `create or replace` statements that you need to run on Snowflake
2. `table_rules.json` file - used for running the batch load.

## Development setup

* [Azure Data Studio](https://docs.microsoft.com/en-us/sql/azure-data-studio/download-azure-data-studio) a cross platform app for connecting to SQL Server (replaces SSMS)
  * Or some other SQL tool like [DBeaver](https://dbeaver.io/) or if you are on Windows, SSMS
* [Docker Desktop](https://www.docker.com/products/docker-desktop) for Mac / Windows or something else to run docker and docker-compose on your OS


### Testing with the local container environment
1. Export environment variables
* _Mac_
```sh
export FN_SQL_SERVER=localhost
export FN_SQL_USER=sa
export FN_SQL_PASSWORD=Fake!News9000
export FN_SQL_PORT=1433
```
* _Windows_

Make sure you can run powershell scripts - run a terminal as admin and then at the prompt:
```powershell
Set-ExecutionPolicy RemoteSigned -Scope CurrentUser -Force
```
Export environment variables
```powershell
[System.Environment]::SetEnvironmentVariable('FN_SQL_SERVER','localhost','User')
[System.Environment]::SetEnvironmentVariable('FN_SQL_USER','sa','User')
[System.Environment]::SetEnvironmentVariable('FN_SQL_PASSWORD','Fake!News9000','User')
[System.Environment]::SetEnvironmentVariable('FN_SQL_PORT','1433','User')
```
_You might need to restart the terminal_ 

There is no Snowflake local endpoint. If you want to test the complete load then you will need a Snowflake account, either an existing one - or it is easy enough to create a 14-day trial account.
You will also need to export environment variables for all of the Snowflake ones (see Setup & Requirements above)
If using a Trial account, you don't need to set the Authenticator, if using OKTA you can set Authenticator to `externalbrowser`.

2.  Launch the MSSQL Server and S3 endpoint containers with [setup_test_environment.sh](setup_test_environment.sh)
> When you are done, shut them down with [teardown_test_environment.sh](teardown_test_environment.sh)

_On Windows use the equivalent powershell scripts_ 
> TODO: Docker on Windows requires Hyper-V to be enabled in BIOS - can't test yet

3. Test the creation of the table_ddl.sql and table_rules.json files
```sh
python -m flakenews -c ./table_config_example.yml
```

4. You can connect to the MSSQL Server machine with sqlcmd, Azure Data Studio, SSMS, DBeaver or other tool with the information as per those environment variables to check the data or run any SQL you like; data will not be persisted when the containers are shut down.

5. Test the S3 service container:
> Requires the [awscli](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html) tool to be installed_ 
* Make a bucket of flake news
`aws --endpoint-url=http://localhost:4566 s3 mb s3://flakenews`

* List buckets
`aws --endpoint-url=http://localhost:4566 s3 ls` 


## Use cases

1. Output Snowflake `create table` DDL statements from a source database and generate a `table_rules.json` file with detailed metadata about tables to load.

    Use a table_config.yml file to restrict to a subset of tables
```sh
python -m flakenews -c ./table_config.yml
```


2. Initial load to snowflake - extract csv files from a source database with splitting at every <batch_size> rows and then uploads files to Snowflake tables
* loads to a staging table then copied to destination
* For now, you would need to truncate the snowflake table to do a full re-load

_Use the table_rules.json created earlier to upload to Snowflake
```sh
python -m flakenews -r ./table_rules.json
```

3. Single table full reload
* Truncate the table on Snowflake 
* backup the `table_rules.json` and `table_config.yml`
* make a new cut-down `table_config_single_table.yml` file and use that to output a new `table_rules.json` file
```sh
python -m flakenews -c ./table_config_single_table.yml`
```

* and then run the initial load with that - it  Does a full load to a transient table then meta-swapped to the destination table?

3. Initial load to S3 - TODO

4. Ongoing load - not implemented, consider Fivetran or other tool first

5. Deploying and running - TBD
* For now this is a tool to run manually in the context of one-off loads
* Extensions are possible, e.g. using MSSQL Server Change Tracking (CT), Rowversion, or other tools, but for now it is stateless. To use one of those ongoing load methods requires keeping state e.g. in Snowflake.
* Options for running on AWS or automating: Buildkite, Fargate, Lambda, Step Functions, AWS MWAA (airflow)
  * Lambda's 15 mins and 512MB /temp is too limiting
  * Step Functions makes it a bit more rubegoldbergian - splits and watermarks across a dynamic loop (array driven) state machine...
  * Fargate has 20GB storage and is long-lived and could do a full reload easily in a kind of stateless way (not counting the data itself as state, and assuming there's no delta-loading, just a merge or swap on Snowflake)
* Logging & Observability - logging has been added in a way that is compatible with AWS Lambda and other AWS services so should appear on Cloudwatch or other connected services. 


### Notes
* For development, testing or manual operation, the simplest connection credentials to use are _Windows Authentication_ rather than username / pass, however both are supported.

* Using SSO web browser auth with Snowflake is hard to use unattended. Web browsers will pop up so the solution is to use the secure-local-storage add on installed along with the snowflake-connection-python.

* Uploading directly to Snowflake e.g. with SnowSQL or similar is not the optimal choice, but then neither is flake news. Snowpipe is often cheaper as it uses managed virtual warehouse clusters for processing and has some other bulk upload optimisations. It also means this tool requires a Snowflake login and the associated secrets management that goes along with it. It does, however, reduce the infrastructure and setup requirements as you don't need an S3 bucket or a Snowpipe object.
