create table if not exists dept
(
    deptno int,
    dname  string,
    loc    int
)
    row format delimited fields terminated by "\t";

create table if not exists emp
(
    empno  int,
    ename  string,
    job    string,
    sal    double,
    deptno int
)
    row format delimited fields terminated by "\t";

load data local inpath "/opt/module/hive/datas/dept.txt" into table dept;
load data local inpath "/opt/module/hive/datas/emp.txt" into table emp;

select
    *
from emp;
select
    empno,
    ename
from emp;
select
    empno as emp_id,
    ename emp_name
from emp;

select
    *
from emp
limit 5;
select
    *
from emp
limit 2,3;

select
    *
from emp
where sal > 1000;

select
    *
from emp
where sal between 500 and 1000;

select
    *
from emp
where job in("研发","销售");

select
    *
from emp
where ename not like "张%";

select
    *
from emp
where job like "研_";

select
    *
from emp
where ename like "张%" or ename like "赵_";


-- 聚合函数
-- count
set mapreduce.framework.name=local; // 设置本地模式为了测试
select
    count(*) cnt
from emp;

select
    count(job) // null值不算
from emp;

-- max min
select
    max(sal)
from emp;
select
    min(sal)
from emp;

-- sum
select
    sum(sal) sum_sal
from emp;

-- avg
select
    avg(sal) avg_sal
from emp;
select
    sum(sal)/count(sal)
from emp;

-- group by
select
    job,
    count(*)
from emp
group by job;

-- 报错
-- [42000][10025] Error while compiling statement:
-- FAILED: SemanticException [Error 10025]: Line 2:4 Expression not in GROUP BY key 'deptno'
select
    deptno,
    job,
    count(*)
from emp
group by job;

-- having
select
    job,
    cnt
from(
    select
    job,
    count(*) cnt
    from emp
    group by job
) t1
where cnt >= 2;


select
    job,
    count(*) cnt
from emp
group by job
having cnt >= 2;

-- join
select
    *
from emp join dept
on emp.deptno = dept.deptno;

select
    loc,
    count(*)
from emp join dept
on emp.deptno = dept.deptno
group by loc;

-- 等值连接&不等值连接
select
    *
from emp join dept
on emp.deptno = dept.deptno;

select
    *
from emp join dept
on emp.deptno != dept.deptno;

-- 内连接
select
    e.empno,
    e.ename,
    d.deptno
from emp e
join dept d
on e.deptno = d.deptno;

-- 左外连接
select
    e.empno,
    e.ename,
    d.deptno
from emp e
left outer join dept d
on e.deptno = d.deptno;

-- 右外连接
select
    e.empno,
    e.ename,
    d.deptno
from emp e
right outer join dept d
on e.deptno = d.deptno;

-- 满外连接
select
    *
from emp e
full outer join dept d
on e.deptno = d.deptno;

-- 多表连接
create table if not exists location(
    loc int,           -- 部门位置id
    loc_name string   -- 部门位置
)
row format delimited fields terminated by '\t';
load data local inpath '/opt/module/hive/datas/location.txt' into table location;

select
    *
from emp e
join dept d
on e.deptno = d.deptno
join location l
on d.loc = l.loc;

select
    e.ename,
    d.dname,
    l.loc_name
from emp e
join dept d
on d.deptno = e.deptno
join location l
on d.loc = l.loc;

-- 笛卡尔积 n*m
-- 1、没有连接条件
-- 2、连接条件无效
-- 3、所有表中的所有行互相连接
select *
from emp join dept;

select *
from emp join dept
on 1=1;

select *
from emp dept;

-- union & union all
select
    *
from emp
where deptno=30
union
select
    *
from emp
where deptno=40;

select
    *
from emp
where deptno=40
union
select
    *
from emp
where deptno=40;

-- order by
set mapreduce.job.reduces;
select
    *
from emp
order by sal asc;

select
    *
from emp
order by sal desc
limit 3;















































