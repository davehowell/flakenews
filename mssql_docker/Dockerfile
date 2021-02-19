FROM mcr.microsoft.com/mssql/server:2017-latest

RUN mkdir -p /usr/src/app

WORKDIR /usr/src/app

COPY . /usr/src/app

RUN chmod +x /usr/src/app/import-data.sh

EXPOSE 1433

ENTRYPOINT ["/bin/bash","./entrypoint.sh"]