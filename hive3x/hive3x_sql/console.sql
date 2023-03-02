-- 创建数据库
create database if not exists db_hive1;

create database db_hive2 location "/db_hive2";

create database db_hive3 with dbproperties ("create_date" = "2023-02-21");

-- 查询数据库
show databases;

show databases like "db*";

describe database db_hive3;

describe database extended db_hive3;

-- 修改数据库
alter database db_hive3 set dbproperties("create_date"="2023/02/21");

-- 删除数据库
drop database db_hive2;

drop database db_hive3 cascade;//强制

-- 切换数据库
use db_hive3;

------------------------------------------------------------------


-- 建表
create table
















