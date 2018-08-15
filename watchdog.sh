#!/bin/sh

export ODBCINI=/opt/teradata/client/ODBC_64/odbc.ini
export ODBCINST=/opt/teradata/client/ODBC_64/odbcinst.ini
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/teradata/client/16.20/odbc_64/lib
/home/stack/watchdog/watchdog.py> /dev/null 2> /tmp/run_errors/watchdog_err.txt