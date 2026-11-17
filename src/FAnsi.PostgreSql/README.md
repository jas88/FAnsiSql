# Postgres FAnsi Implementation

# Feature Completeness

- Server
  - [X] Check Exists
  - [ ] Describe 
  - [X] List Databases

- Database
  - [X] Create
  - [X] Drop
  - [ ] Backup
  - [ ] Detach
  - [X] List Tables
  - [ ] List Table Valued Functions
  - [ ] List Stored Proceedures
  
- Table 
  - [X] Create
  - [X] Drop
  - [X] Script Table Structure
  - [X] MakeDistinct
  - [X] Bulk Insert
  - [X] Rename
  - [X] List Foreign Keys
  
- Column
  - [X] Alter
  
- Data Types
  - [X] [Translation](./../../Documentation/TypeTranslation.md)
  - [X] Bit
  - [X] String
  - [X] TimeSpan
  - [X] Decimal
  - [X] Date
  - [X] Auto Increment
  - [X] Unicode
  
- Query
  - [X] Top X
  - [X] JOIN UPDATE
  
- Aggregation 
  - [X] Basic GROUP BY
  - [X] Calendar/Axis Table GROUP BY
  - [X] Dynamic Pivot GROUP BY
  

##Issues
A query cannot choose its own columns, so the SQL for a dynamic pivot is a `DO` block (PL/pgSQL) that opens a cursor called `fansi_pivot`, followed by `FETCH ALL FROM fansi_pivot` and `CLOSE fansi_pivot`.  Run it all on one connection, e.g. as a single command.
