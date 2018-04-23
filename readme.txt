Welcome to Davisbase!

The project supports Java 8 run time environment.

Please find attached sample SQL file and screenshots for your reference.

Steps:
------
1. javac Start.java;
2. java Start;
3. hasanth> [SQL COMMANDS];

MetaTables
----------
1. hasanth_tables
2. hasanth_columns


Supported Commands
------------------
1. Show tables;
2. Create table;
3. Drop table <tableName>;

4. Insert into <tableName> [column_list] values [value_list];
5. Update <tableName> set column_name = value [where condition];
6. Delete from <tableName> where condition;

7. Select * from where condition;
8. Exit;

Assumptions
-----------
1. Inclusion of row_id is not mandatory as row_id is auto incremented for insert and create commands.
2. Constraints considered are NULL, NOT NULL and PRIMARY KEY.
3. Operators supported are >,< and =.
4. All the tables are present in the data table only.
5. Overflow for hasanth_columns is supported.
6. Record Formats supported are INT, TEXT, DOUBLE and NULL.