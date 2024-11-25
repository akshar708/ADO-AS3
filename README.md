
# CS_525 Assignment 3 - Record Manager


The record manager manages tables with fixed schema. With this user can insert, delete, update and scan through the records in the tables. With the help of search condition this scan works and only returns records that match the certain search condition. In this each table is stored in separaate page file. This record manager should be able to access pages by buffer manager from asignment 2.

## Running Tests

To build the record manager as well the first set of test cases in test_assign3_1.c, use
./test_assign2_1.o
```bash
make
./test_assign3_1.o
```
To build the second set of test cases in test_assign3_2.c, use run the following command -
```bash
make test_assign3_2
./test_assign3_2.o
```
To clean the solution use
```bash
make clean
```

## Solution Description
1] Record representation-

In this data types are all fixed length. So, for each given shcema size of the records are fixed too. 

2] Tables.h

Defines basic data structure for records, schemas, recoid_id, values and tables. There are four data types that can be usd for records in the table: integer(DT_INT), float (DT_FLOAT), boolean(DT_BOOL) and strinf of fixed length (DT_STRING). 


3] Schema

Schema consists of number of attributes and data types.

4] expr.h

This header defines functions and data structures to deal with scans. Expressions can be constants, references to attribute values and operator invocations. Operators are comparison operators. 

5] record_mgr.h

functions for tables and record manager management, handling records in table, related to scan, dealing with attributes values, schemas and creating records.


```bash
RM_TableData
```
To interact with table.
```bash
getNumTuples
```
 It returns the no of tuples in the table.

```bash
RM_SystemCatalog
```
The system schema is present in first page in the page file . The catalog can be grabbed by casting the raw data of the page by this.
It has 3 int metadata as 'totalNumPages' - Number of pages in the file which will grow, 'freePage' - index of first free page or no page if its empty, 'numTables' - number of tables in system.

```bash
RM_SystemSchema
```
defines the system table schemas on the catalog page. Its attribute, name length, counts and key counts, are limited so that the page can simply be casted for access.

```bash
RM_PageHeader
```
It has 3 int metadata. 'nextPage', 'prevPage', 'numSlots'.

```bash
RC initRecordManager(void *mgmtData)
```
mgmtData names the page file used by the record manager. For NULL the default name DATA.bin is used
If the page file doesn't exist, it is created and the catalog page is initialized.
It starts the buffer pool
This catalog page is always pinned until the record manager is shutdown.

```bash
RC shutdownRecordManager()
```
It unpin the catalog page. It will shut down buffer pool. If any table is still open, the buffer pool will not shutdown

```bash
RC createTable(char *name, Schema *schema)
```
It will scan page catalog for an existing table with name. If a table is already there, the operation will fail.
This makes sure the system is not already at MAX_NUM_TABLES and the new schema matches the system's requirements
The table is created
The attributes are added to system schema as well
a free page is taken and its slot array is set to FALSE which indicates all slots are free

```bash
RC openTable(RM_TableData *rel, char *name)
```
With this table is open and first page is pinned
the mgmtData of the RM_TableData also points back to the system schema
```bash
RC closeTable(RM_TableData *rel)
```

It unpins page and force flushes and frees the malloc from openTable()

```bash
RC deleteTable (char *name)
```
This must be called on a closed table that with name exists.
System schema removes that table from its entry.
Page (and its overflow pages) are appended onto the free list

```bash
RC insertRecord (RM_TableData *rel, Record *record)
```
It gives slots for table, looking for an opening, starting with slots on its main page
Overflow pages are then used
If there is no free space, a free page is taken

```bash
RC deleteRecord (RM_TableData *rel, RID id)
RC updateRecord (RM_TableData *rel, Record *record)
RC getRecord (RM_TableData *rel, RID id, Record *record)
```
This gives slot on id.slot on page id.page and does its work

```bash
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
RC next (RM_ScanHandle *scan, Record *record)
RC closeScan (RM_ScanHandle *scan)
```
This scans only main page of the table and not overflow pages.

```bash
RC createRecord (Record **record, Schema *schema)
RC freeRecord (Record *record)
```
This allocates memory for the both the record as well as the record's data. It is useful for creating records on insertion

```bash
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
```
It will return a Value on the column attrNum in the Record. This value's data is also malloc.
The caller is responsible for calling freeVal


```bash
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
```
This sets data for existing Value from a Record column attribute


## Authors



Nisha Prajapati - A20585829