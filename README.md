# StaxSQL
A SQL engine written in python capable of parsing and executing medium-complexity queries.

## Instructions
_NOTE:_ Column name should not start with int, and should not contain a `.`  
<br/>
Handles the following cases of query processing:
- Blank column entries while entering data from data_file.  
- Multiple AND or OR queries, including nested ones.  
- Handling operators like `=, !=, >, <, <=, >=` in column comparison.  
- Handling queries of the form:
  - **The case of the DML language does not matter.**
  - SELECT select from A;
  - SELECT * from table1,table2 WHERE A = "234" OR (B=23 AND C=D);
  - SELECT *,A from A,B WHERE A = 234;
  - SELECT * from table1 as table2;
  - SELECT * from table1
  - SELECT max(A) from table1
  - SELECT min(B) from table1
  - SELECT avg(C) from table1
  - SELECT sum(D) from table2
  - SELECT A from table1
  - SELECT A,D from table1,table2
  - SELECT distinct(C) from table3
  - SELECT B,C from table1 where A=-900
  - SELECT A,B from table1 where A=775 OR B=803
  - SELECT A,B from table1 where A=922 OR B=158;
  - SELECT * from table1,table2 where table1.B=table2.B
  - SELECT A,D from table1,table2 where table1.B=table2.B
  - SELECT A from table4;
  - SELECT Z from table1;
- Aggregate function like, (distinct, sum, min, max, avg)
- Pretty Table output
- Summary of successful query
- Handle table alaising


<!-- - Differenet character types -->


## License
<p xmlns:dct="http://purl.org/dc/terms/" xmlns:cc="http://creativecommons.org/ns#" class="license-text"><a rel="cc:attributionURL" property="dct:title" href="github.com/pinkeshbadjatiya/StaxSQL">StaxSQL</a> by <a rel="cc:attributionURL dct:creator" property="cc:attributionName" href="pinkeshbadjatiya.github.io">Pinkesh Badjatiya</a> is licensed under <a rel="license" href="https://creativecommons.org/licenses/by-nc/4.0">CC BY-NC 4.0 <img style="height:22px;margin-left:3px;vertical-align:text-bottom;" src="https://mirrors.creativecommons.org/presskit/icons/cc.svg?ref=chooser-v1" /> <img style="height:22px;margin-left:3px;vertical-align:text-bottom;" src="https://mirrors.creativecommons.org/presskit/icons/by.svg?ref=chooser-v1" /> <img style="height:2px; width: 2px; margin:30px; vertical-align:text-bottom;" src="https://mirrors.creativecommons.org/presskit/icons/nc.svg?ref=chooser-v1" /> </a></p>
