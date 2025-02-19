#!/usr/bin/env bash

echo "FAKE FAKE NEWS"

#run the setup script to create the DB and the schema in the DB
#do this in a loop because the timing for when the SQL instance is ready is indeterminate
for i in {1..50};
do
    /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "${SA_PASSWORD}" -d master -i setup.sql
    if [ $? -eq 0 ]
    then
        echo "setup.sql completed"
        break
    else
        echo "not ready yet..."
        sleep 1
    fi
done

#import the fake data from the tsv file
/opt/mssql-tools/bin/bcp DemoData.dbo.Fake_Data in "/usr/src/app/fake_data.csv" -c -C 65001 -t'|' -S localhost -U sa -P "${SA_PASSWORD}"

#bulk up the fake data with brotein
/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "${SA_PASSWORD}" -d master -i multiply_data.sql