# StaxSQL
A SQL engine written in python capable of parsing and executing medium-complexity queries.

- NOTE: Column name should not start with int, should not contain "."

Handles the following cases of query processing:
- Blank column entries while entering data from data_file.  
- Multiple AND or OR queries, including nested ones.  
- Handling operators like =, !=, >, <, <=, >= in column comparison.  
- Handling queries of the form:
  - The case of the DML language does not matter.
  - SELECT select from A;
  - SELECT * from table1,table2 WHERE A = "234" OR (B=23 AND C=D);
  - SELECT *,A from A,B WHERE A = 234;
  - SELECT * from table1 as table2;
- Handle table alaising
- Pretty Table output
- Summary of successful query





<!-- - Differenet character types -->
