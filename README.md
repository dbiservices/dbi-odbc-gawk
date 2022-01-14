# odbc-gawk

odbc-gawk is a gawk's extension for accessing any data sources via ODBC through their ODBC drivers.

## Description

odbc-gawk lets gawk scripts connect to any data source for which an ODBC driver is available and send it SQL CRUD statements to retrieve, modify, or delete data.
Data sources can be RDBMS (relational database management systems) or not (NoSQL, Excel spreadsheets, etc.) as long as a suitable ODBC driver is available for them.
Free, open source RDBMS such as SQLite, MariaDB and PostgreSQL provide such ODBC drivers, and so do commercial ones. Companies such as Devart, cdata, Progress and Easysoft sell drivers for the well-known data sources but also for more exotic ones such as PayPal, MongoDB, CRMs and Excel spreadsheets.
odbc-gawk has been written for linux and the GCC compiler suite.

## Getting Started

### Dependencies

odbc-gawk has been written and tested under Debian v11 (bullseye) linux.
It currently extends gawk v5.1.0 but should do so for any version of gawk >= v4.1 where the dynamic extension interface is available.
It relies on the ODBC Driver Manager and ODBC drivers for each data source to be accessed. Those can mostly be installed trhough the official package management tool such apt for Debian-derivatives and yum for RedHat derivatives.

### Installing

#### As root, install the following packages:
* apt-get install libodbc1
* apt install unixodbc

#### As a non-root user, e.g. user debian with home dir /home/debian, run the commands below:
```
mkdir ~/odbc-gawk
cd ~/odbc-gawk
git clone https://github.com/dbiservices/dbi-odbc-gawk.git
cd gawk-5.1.0/extension

export LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/odbc:/home/debian/instantclient_21_3:$LD_LIBRARY_PATH
export PATH=/opt/mssql-tools/bin:$PATH
export NLS_LANG=american_america.UTF8
export AWKLIBPATH=/home/debian/odbc4gawk/odbc-gawk/gawk-5.1.0/extension/.libs

make && cd .libs && gcc -o odbc.so --shared odbc.o /usr/lib/x86_64-linux-gnu/libodbc.so && cd ../.. && rm gawk && make
```

The new gawk executable is still in gawk-5.1.0. To install it system-wide, use:
```
make install
```
from there as root .

### Executing program

A test program, todbc.awk, is provided in gawk-5.1.0/extension to show how to use the ODBC extension. To use it:
```
cd extension
../gawk -f ./todbc.awk
```

## Author

Cesare Cervini, cesare.cervini at dbi-services.com

## Version History

* 0.1
    * Initial Release

## License

This project is free to use, do what you want with it. I hope that it will be useful. Remarks and suggestions are welcome.

## Acknowledgments

The fathers of the awk language Alfred V. Aho, Brian W. Kernighan, and Peter J. Weinberger for their incredibly smart and elegant creation.
All the gawk contributors, and especially Arnold D. Robbins, for their outstanding work on the gawk interpreter and the excellent book "GAWK: Effective AWK Programming".

