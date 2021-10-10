# Functional tests on Apache-Hive

## Design Notes
All tests from the original repo will be migrated.
Hive containers will be shared for all tests but there will not be any dependencies between them.
     
# Folder structures
## Local file system
### resources folder
Will contains all mappings used to generate sql scripts

### tmp folder
Will contain generated sql.
This folder will be purged before running tests.

### tests folder
Contains ``docker-compose.yml`` and ``hadoop-hive.env``

#### tests_data folder
This folder is mapped to hive-server.
Will contain all test json data.
Test data is copied to hdfs via ``hdfs dfs -put`` during tests startup.

## HDFS
Physical location of HDFS folders is in the local ``tests`` folder.

Each test has its own location. Raw JSON test data is copied to folder ``/user/hive/source/{test-name}/``.

Where sourceDB,sourceCollection come from mapping files. Both will be aligned to 

The database user is _hive_ user and _default_ database is the testing database, hence the folder where managed tables 
 will be stored is here:
``
/user/hive/warehouse/default/
``

## Database and tables
External table is created on top of raw json files in '/user/hive/{test-name}'

Managed tables are produced 
Each test will produce its unique, hive-managed table in default database, hence data from raw


# Test Flow
Start docke-compose and wait till it is ready for 60 seconds.

Copy/overwrite test data from local folder mapped to  ``/tests_data/`` to hdfs ``/user/hive/source/{test-name}/``



