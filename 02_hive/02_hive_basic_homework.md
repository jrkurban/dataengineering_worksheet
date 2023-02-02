# Questions

### Q-1: 
- Create a hive database `hive_odev` and load this data https://raw.githubusercontent.com/erkansirin78/datasets/master/Wine.csv into `wine` table.

### A-1
1- create database hive_odev,   
2- use hive_odev,
3-create table if not exists wine
(Alcohol float, Malic_Acid float, Ash float, Ash_Alcanity float, Magnesium float, Total_Phenols float, Flavanoids float, Nonflavanoid_Phenols float, Proanthocyanins float, Color_Intensity float, Hue float, OD280 float, Proline float, Customer_Segment int)
row format delimited
fields terminated by ','
lines terminated by '\n'
tblproperties('skip.header.line.count'='1');
4-hdfs dfs -put ~/datasets/Wine.csv /user/train/hdfs_odev
5-load data inpath '/user/train/hdfs_odev/Wine.csv' into table wine;

### Q-2
- In `wine` table filter records that `Alcohol`greater than 13.00 then insert these records into `wine_alc_gt_13` table.

### A-3

create table wine_alc_gt_13 as select * from wine where wine.alcohol > 13.00;

### Q-3
- Drop `hive_odev` database including underlying tables in a single command.

### Q-4 
- Load this https://raw.githubusercontent.com/erkansirin78/datasets/master/hive/employee.txt into table `employee` in `company` database. 

### A-4
- drop database hive_odev cascade
-create database company   use company
-create table employee (name string,work_place array<string>,gender_age struct <gender:string,age:int>,skills_score map<string,int>)
row format delimited
fields terminated by '|'
collection items terminated by ','
map keys terminated by ':'
stored as textfile
tblproperties('skip.header.line.count'='1');
-wget https://raw.githubusercontent.com/erkansirin78/datasets/master/hive/employee.txt
-hdfs dfs -put ~/datasets/employee.txt /user/train/hdfs_odev
-load data inpath '/user/train/hdfs_odev/employee.txt' into table employee;

### Q-5
- Write a query that returns the employees whose Python skill is greater than 70.

### A-5

-select * from employee where skills_score["Python"] > 70;