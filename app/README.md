# Partitioning script generator

The purpose of this script is to generate a script (yup, recursion) to the partition table.
The only last step (as for now) is to add partition part for the table generation script. 
Please use the provided min/max script to get your minimal partition start date.
This script will **not work** (by the purpose) for already partitioned tables.

## Getting Started

Please check out this script as well as supported scripts from GIT. Please do not work in your master repository folder and do not push changes to the repository unless you talked to the developer of this script.
Please use common sense before dropping objects in any environment.

### Prerequisites

Required: Python 3

Packages:

```
loguru
cx_Oracle
```

### Usage

#### Config file for DB connection
Config file for DB connection is config.py. Please edit this file accordingly.

```
database_connection = {
    "connection_type": "direct",
    "host_name": "localhost",
    "service_name": "XE",
    "port": "1521"
}
```

#### Command line parameters
```
magic.py --table TABLE_TEST_DT --owner test --user test --password test
```

These parameters are self-explanatory. 

#### Result:
As a result, you will have the SQL script generated.
After you add one partition to the table part, you can run the script step by step. Please **note** that some of the big tables can take a lot of time and space to copy to the newly created table.

After you have at least one partition you can use partition_merlin script to add partitions to your table. Remember, that minimal partition should fit your requirements for the minimal value of the date. 



## Acknowledgments

* Hat tip to anyone whose code was used
* Inspiration: laziness to do this by hand
* Please report bug/feature requests to the author/support group



