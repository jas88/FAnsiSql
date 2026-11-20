# MySql FAnsi Implementation

Works with MySQL and MariaDB, both through `DatabaseType.MySql`. Calendar aggregates use recursive CTEs, so they need
MySQL 8.0 or later, or MariaDB 10.2 or later. CI tests MySQL 8.0 and MariaDB 12.3 (LTS); older versions are not
tested.

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
  - [ ] Unicode

- Query
  - [X] Top X
  - [X] JOIN UPDATE
  
- Aggregation 
  - [X] Basic GROUP BY
  - [X] Calendar/Axis Table GROUP BY
  - [X] Dynamic Pivot GROUP BY
