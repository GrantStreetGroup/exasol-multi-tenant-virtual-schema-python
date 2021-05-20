# py-exasol-virtual-schema

A python implementation of the [Exasol Java Virtual Schema Adapter](https://github.com/exasol/exasol-virtual-schema) with added support for multi-schema multi-tenancy.

It is faster than the equivalent Java implementation (~1s faster per query).

## Examples

### Single remote schema

First create a connection to the remote Exasol instance where your schema resides:
```sql
CREATE CONNECTION REMOTE_EXASOL TO '...:8563' USER '...' IDENTIFIED BY ...;
```

Next create an adapter script containing the contents of `adapter.py`:
```sql
--/
CREATE OR REPLACE PYTHON ADAPTER SCRIPT py_vs_adapter AS
    %paste the contents of adapter.py here%
/;
```

Finally create your virtual schema:
```sql
CREATE VIRTUAL SCHEMA test_virtual
    USING py_vs_adapter
    WITH CONNECTION_NAME = 'REMOTE_EXASOL'
         SCHEMAS = 'REMOTE_SCHEMA_NAME'
;
```

### Multiple local schemas

This is useful in a multi-tenancy setup where each tenant has a separate schema and you would like to query data from one or more of the schemas in a single statement. If querying multiple schemas the results are UNIONed into a single resultset.

In this example suppose you have 3 tenants each with their own identical schema: `TNT_ONE`, `TNT_TWO`, and `TNT_THREE`

First create a connection to the local Exasol instance where your schemas reside:
```sql
CREATE CONNECTION LOCAL_EXASOL TO '...:8563' USER '...' IDENTIFIED BY ...;
```

Next create an adapter script containing the contents of `adapter.py` (same as previous example):
```sql
--/
CREATE OR REPLACE PYTHON ADAPTER SCRIPT py_vs_adapter AS
    %paste the contents of adapter.py here%
/;
```

Finally create your virtual schema:
```sql
CREATE VIRTUAL SCHEMA tnt_all
    USING py_vs_adapter
    WITH CONNECTION_NAME = 'LOCAL_EXASOL'
         SCHEMAS = 'TNT_ONE,TNT_TWO,TNT_THREE'
         IS_LOCAL = 'TRUE'
         TENANT_PATTERN = 'TNT_(.+)' -- causes CLIENT column to contain ONE, TWO, THREE
         TENANT_COLUMN  = 'CLIENT'
;
```

The virtual schema `TNT_ALL` will now appear to have the same schema as the underying `TNT_*` schemas and if you query it you'll get a resultset that is the UNION of all three schemas.

Each table in the virtual schema will have an additional virtual column called `CLIENT` (or whatever you specify in the `TENANT_COLUMN` property.) You can specify criteria against any one of these virtual `CLIENT` columns in order to filter which clients you wish to query.

```sql
SELECT col1, col2
FROM tnt_all.tbl1
JOIN tnt_all.tbl2
  ON tbl1.id = tbl2.id
WHERE tbl1.client IN ('ONE','THREE')
```

Alternatively you can restrict the resultset to a single client based on the current username by specifying an additional `TENANT_USERS = 'TNT_(.+)_USER.*'` property in the virtual schema. If this property is set and if the current username is, for example, `TNT_TWO_USER1` then that user will only get results from the `TNT_TWO` schema. This is useful for having a single view defined that can be used by any of the tenants.

See the comments in `adapter.py` for more details.

# Author

Grant Street Group <developers@grantstreet.com>

# Copyright and License

This software is Copyright (c) 2021 by Grant Street Group.

This is free software, licensed under:

    MIT License

# Contributors

- Peter Kioko <peter.kioko@grantstreet.com>


