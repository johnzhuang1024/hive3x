set mapreduce.framework.name=local;

-- 分区表
-- 建表语句
drop table dept_partition;
create table if not exists dept_partition
(
    deptno int,    --部门编号
    dname  string, --部门名称
    loc    string  --部门位置
)
    partitioned by (day string)
    row format delimited fields terminated by '\t';

-- 写
-- load
load data local inpath '/opt/module/hive/datas/dept_20220401.log'
    into table dept_partition
    partition (day = '20220401');

select *
from dept_partition;

-- insert
insert overwrite table dept_partition partition (day = "20220402")
select deptno,
       dname,
       loc
from dept_partition
where day = "20220401";

select *
from dept_partition;

-- 分区表基本操作
-- 查看所有分区信息
show partitions dept_partition;

-- 增加分区
alter table dept_partition
    add partition (day = "20220403");

select *
from dept_partition
where day = "20220403";

alter table dept_partition
    add partition (day = "20220404") partition (day = "20220405");

-- 删除分区
alter table dept_partition
    drop partition (day = "20220403");

alter table dept_partition
    drop partition (day = "20220404"),partition (day = "20220405");
// 有逗号


-- 二级分区
create table dept_partition2
(
    deptno int,    -- 部门编号
    dname  string, -- 部门名称
    loc    string  -- 部门位置
)
    partitioned by (day string, hour string)
    row format delimited fields terminated by '\t';

load data local inpath '/opt/module/hive/datas/dept_20220401.log'
    into table dept_partition2
    partition (day = '20220401', hour = '12');

-- 动态分区
create table dept_partition_dynamic
(
    id   int,
    name string
)
    partitioned by (loc int)
    row format delimited fields terminated by '\t';

set hive.exec.dynamic.partition.mode = nonstrict;
insert overwrite table dept_partition_dynamic partition (loc)
select deptno,
       dname,
       loc
from default.dept;

select *
from dept_partition_dynamic;

show partitions dept_partition_dynamic;

-- 分桶表
-- 建表
create table stu_buck(
    id int,
    name string
)
clustered by(id)
into 4 buckets
row format delimited fields terminated by "\t";

load data local inpath '/opt/module/hive/datas/student.txt'
into table stu_buck;

-- 分桶排序表
create table stu_buck_sort(
    id int,
    name string
)
clustered by(id) sorted by(id)
into 4 buckets
row format delimited fields terminated by '\t';

load data local inpath '/opt/module/hive/datas/student.txt'
into table stu_buck_sort;



























