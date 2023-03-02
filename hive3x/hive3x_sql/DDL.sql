select cast("11.1" as int);

show databases ;

-- 案例一
-- 内部表
create table if not exists student(
    id int comment "id",
    name string comment "name"
)
row format delimited fields terminated by "\t"
location "/user/hive/warehouse/student";

select * from student;

drop table student;

-- 外部表
create external table if not exists student(
    id int,
    name string
)
row format delimited fields terminated by "\t"
location "/user/hive/warehouse/student"

drop table student;

-- SERDE和复杂数据类型SERDE和复杂数据类型
create table if not exists teacher(
    name string,
    friends Array<string>,
    students map<string,int>,
    address struct<street:string,city:string,postal_code:int>
)
row format serde "org.apache.hadoop.hive.serde2.JsonSerDe"
location "/user/hive/warehouse/teacher";

drop table teacher;

select * from teacher;

select friends[0],students["xiaohaihai"],address.street from teacher;

-- create table as select和create table like
create table teacher2 as select * from teacher;

select * from teacher2;

create table teacher3 like teacher;


-- 查看表
show tables like "stu*";

show tables in db_hive3 like "stu*";

describe teacher;
describe extended teacher;
describe formatted teacher;

-- 修改表
alter table stu rename to stu1;

set hive.metastore.disallow.incompatible.col.type.changes = false;

desc stu1;

alter table stu1 add columns (gender string);

alter table stu1 change column gender gender int after id;

alter table stu1 replace columns (id int, name string);


-- 删除表

-- 清空表

-- DML（Data Manipulation Language）数据操作
-- Load
create table student(
    id int,
    name string
)
row format delimited fields terminated by '\t';

load data local inpath '/opt/module/datas/student.txt' into table student;

load data local inpath '/opt/module/datas/student.txt' overwrite into table student;

load data inpath '/user/atguigu/student.txt'
into table student;

-- insert
create table student1(
    id int,
    name string
)
row format delimited fields terminated by '\t';

insert overwrite  table student1
select * from student;

insert into table  student1 values(1,'wangwu'),(2,'zhaoliu');

insert overwrite local directory '/opt/module/datas/student' ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
select id,name from student;

-- Export&Import
export table default.student to '/user/hive/warehouse/export/student';

import table student2 from '/user/hive/warehouse/export/student'

