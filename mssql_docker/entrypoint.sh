#start SQL Server, start the script to create the DB and import the data
/opt/mssql/bin/sqlservr & /usr/src/app/import-data.sh & tail -f /dev/null