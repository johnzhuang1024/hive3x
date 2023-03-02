-- 创建functions数据库
create database fuctions;

-- 展示functions
show functions like "*string*";
desc function substring;
desc function extended substring;

-- 单行函数
-- 算术运算函数
select 1 & 0;
select 3 & 2;
select 3 | 2;
select 3 ^ 2;
select ~3;

-- 数值函数
select round(3.5);
select round(3.53, 1);
select round(-1.5);
select ceil(1.5);
select `floor`(1.5);

-- 字符串函数
desc function substring;
select substring("atguigu", 0, 3);
select substring("atguigu", -3, 2);

select replace("atguigu", "u", "U");

select regexp_replace("abcd-100-200-abc", "\\d+", "*");

select "string" regexp "s.";
select "string" like "%str%";

select repeat("str", 3);

select split("192.168.204.100", "\\.");

select nvl(1, 0);
select nvl(null, 0);

select concat("str", "ing");
select "aa" || "*" || "bb";

select concat_ws("*", "str", "ing");
select concat_ws("*", `array`("aa", "bb", "cc"));

select get_json_object('[{"name":"大海海","sex":"男","age":"25"},{"name":"小宋宋","sex":"男","age":"47"}]',
                       '$[0].name');

-- 日期函数
select unix_timestamp();
select unix_timestamp("2000/08/21 12-00-00", "yyyy/MM/dd HH-mm-ss");

select from_unixtime(1677334203, "yyyy/MM/dd HH:mm:ss");

select from_utc_timestamp(1677334203.000, "GMT+8");
select from_utc_timestamp(cast(1677334203 as bigint) * 1000, "GMT+8");
select date_format(from_utc_timestamp(cast(1677334203 as bigint) * 1000, "GMT+8"), "yyyy/MM/dd HH:mm:ss");

select `current_date`();

select `current_timestamp`();

select datediff("2023-02-25", "2000-08-21");

select date_add("2000-8-21", 8223);

select date_sub("2023-02-25", 8223);

-- 流程控制函数
-- case
select stu_id,
       course_id,
       case
           when score >= 90 then "A"
           when score >= 80 then "B"
           when score >= 70 then "C"
           when score >= 60 then "D"
           else "不及格"
           end
from score_info;

select stu_id,
       course_id,
       case score
           when 90 then "A"
           when 80 then "B"
           when 70 then "C"
           when 60 then "D"
           else "不及格"
           end
from score_info;

-- if
select `if`(1 > 2, "true", "false");


-- 集合函数
select `array`(1, 2, 3);

select array_contains(`array`("a", "b", "c", "d"), "a");

select sort_array(`array`("a", "b", "z", "d"));

select size(`array`("a", "b", "c", "d"));

select `map`("xiaohong", 1, "dahai", 2);

select map_keys(`map`("xiaohong", 1, "dahai", 2));
select map_values(`map`("xiaohong", 1, "dahai", 1));

select struct("name", "age", "weight");
select named_struct("name", "xiaohong", "age", 12);


-- test
set mapreduce.framework.name=local;
create table employee
(
    name     string,         --姓名
    sex      string,         --性别
    birthday string,         --出生年月
    hiredate string,         --入职日期
    job      string,         --岗位
    salary   double,         --薪资
    bonus    double,         --奖金
    friends  array<string>,  --朋友
    children map<string,int> --孩子
);
insert into employee
values ('张无忌', '男', '1980/02/12', '2022/08/09', '销售', 3000, 12000, array('阿朱', '小昭'),
        map('张小无', 8, '张小忌', 9)),
       ('赵敏', '女', '1982/05/18', '2022/09/10', '行政', 9000, 2000, array('阿三', '阿四'), map('赵小敏', 8)),
       ('宋青书', '男', '1981/03/15', '2022/04/09', '研发', 18000, 1000, array('王五', '赵六'),
        map('宋小青', 7, '宋小书', 5)),
       ('周芷若', '女', '1981/03/17', '2022/04/10', '研发', 18000, 1000, array('王五', '赵六'),
        map('宋小青', 7, '宋小书', 5)),
       ('郭靖', '男', '1985/03/11', '2022/07/19', '销售', 2000, 13000, array('南帝', '北丐'),
        map('郭芙', 5, '郭襄', 4)),
       ('黄蓉', '女', '1982/12/13', '2022/06/11', '行政', 12000, null, array('东邪', '西毒'),
        map('郭芙', 5, '郭襄', 4)),
       ('杨过', '男', '1988/01/30', '2022/08/13', '前台', 5000, null, array('郭靖', '黄蓉'), map('杨小过', 2)),
       ('小龙女', '女', '1985/02/12', '2022/09/24', '前台', 6000, null, array('张三', '李四'), map('杨小过', 2));

select *
from employee;


-- 统计每个月的入职人数
select month(replace(hiredate, "/", "-"))        as month,
       count(month(replace(hiredate, "/", "-"))) as cnt
from employee
group by month(replace(hiredate, "/", "-"));

-- 查询每个人的年龄（年 + 月）
-- 方法一
select name,
       concat(if(month(replace(birthday, "/", "-")) < month(`current_timestamp`()),
                 year(`current_timestamp`()) - year(replace(birthday, "/", "-")) - 1,
                 year(`current_timestamp`()) - year(replace(birthday, "/", "-"))), "年",
              if(month(replace(birthday, "/", "-")) < month(`current_timestamp`()),
                 month(replace(birthday, "/", "-")) - month(`current_timestamp`()) + 12,
                 month(replace(birthday, "/", "-")) - month(`current_timestamp`())), "月") as age
from employee;

-- 方法二
select name,
       concat(if(month >= 0, year, year - 1), '年', if(month >= 0, month, 12 + month), '月') age
from (select name,
             year(current_date()) - year(t1.birthday)   year,
             month(current_date()) - month(t1.birthday) month
      from (select name,
                   replace(birthday, '/', '-') birthday
            from employee) t1) t2;


-- 按照薪资，奖金的和进行倒序排序，如果奖金为null，置位0
desc employee;
select name,
       (salary + nvl(bonus, 0)) as sal
from employee
order by sal desc;

-- 查询每个人有多少个朋友
select name,
       size(friends) cnt
from employee;

-- 查询每个人的孩子的姓名
select name,
       map_keys(children)
from employee;

-- 查询每个岗位男女各多少人
select job,
       sum(if(sex = "男", 1, 0)) male,
       sum(if(sex = "女", 1, 0)) female
from employee
group by job;


-- 高级聚合函数
select collect_list(job),
       collect_set(job)
from employee;

-- 每个月的入职人数以及姓名
select substring(hiredate, 0, 7),
       count(*),
       collect_list(name)
from employee
group by substring(hiredate, 0, 7);

-- 练习题
-- 3.3.3 [课堂讲解]查询同姓（假设每个学生姓名的第一个字为姓）的学生名单并统计同姓人数大于2的姓
select substring(stu_name, 0, 1),
       collect_list(stu_name),
       count(*)
from student_info
group by substring(stu_name, 0, 1)
having count(*) >= 2;

-- 3.4.2 [课堂讲解]按照如下格式显示学生的语文、数学、英语三科成绩，没有成绩的输出为0，按照学生的有效平均成绩降序显示
-- 学生id 语文 数学 英语 有效课程数 有效平均成绩
select stu_id,
       sum(if(ci.course_name = '语文', score, 0)) `语文`,
       sum(if(ci.course_name = '数学', score, 0)) `数学`,
       sum(if(ci.course_name = '英语', score, 0)) `英语`,
       count(*)                                   `有效课程数`,
       avg(score)                                 `平均成绩`
from score_info s1
         join course_info ci
              on s1.course_id = ci.course_id
group by stu_id
order by `平均成绩` desc;



-- 4.1.1 [课堂讲解]查询所有课程成绩均小于60分的学生的学号、姓名
-- 注意学生一共5门课！！
select student_info.stu_id,
       stu_name
from (select stu_id,
             sum(if(score >= 60, 1, 0)) flag
      from score_info
      group by stu_id
      having flag = 0) t1
         join student_info
              on t1.stu_id = student_info.stu_id;


-- 5.1.1 [课堂讲解]查询有两门以上的课程不及格的同学的学号及其平均成绩
select student_info.stu_id,
       avg_sco
from (select stu_id,
             sum(if(score < 60, 1, 0)) flag,
             avg(score)                avg_sco
      from score_info
      group by stu_id
      having flag >= 2) t1
         join student_info
              on t1.stu_id = student_info.stu_id;

-- 5.2.6 [课堂讲解]查询学过“李体音”老师所教的所有课的同学的学号、姓名
select stu_id,
       count(*) cnt
from (select course_id
      from course_info
               join teacher_info ti on course_info.tea_id = ti.tea_id
      where tea_name = "李体音") t1
         join score_info
              on score_info.course_id = t1.course_id
group by stu_id
having cnt = 2;

-- 5.2.7 [课堂讲解]查询学过“李体音”老师所讲授的任意一门课程的学生的学号、姓名
select distinct t2.stu_id,
                stu_name
from (select *
      from (select course_id
            from course_info
                     join teacher_info ti on course_info.tea_id = ti.tea_id
            where tea_name = "李体音") t1
               join score_info
                    on score_info.course_id = t1.course_id) t2
         join student_info
              on t2.stu_id = student_info.stu_id
order by stu_id asc;

-- 5.2.8 [课堂讲解]查询没学过"李体音"老师讲授的任一门课程的学生姓名
-- 方法一
select stu_name
from (select stu_id,
             array_contains(collect_list(tea_name), "李体音") is_con
      from (select *
            from score_info
                     full join course_info ci
                               on score_info.course_id = ci.course_id) t1
               join teacher_info
                    on t1.tea_id = teacher_info.tea_id
      group by stu_id) t2
         right join student_info
                    on t2.stu_id = student_info.stu_id
where nvl(is_con, false) = false;

-- 方法二
select stu_name
from student_info
where stu_id not in
      (select stu_id
       from score_info
       where course_id in
             (select course_id
              from course_info
                       join teacher_info ti
                            on course_info.tea_id = ti.tea_id
              where tea_name = "李体音"));

-- 5.2.9 [课堂讲解]查询至少有一门课与学号为“001”的学生所学课程相同的学生的学号和姓名
-- 方法一
select student_info.stu_id,
       stu_name
from student_info
         join
     (select distinct stu_id
      from score_info
      where course_id in
            (select course_id
             from score_info
             where stu_id = "001")
        and stu_id != "001") t1
     on t1.stu_id = student_info.stu_id;

// 方法二
select si.stu_id,
       si.stu_name
from score_info sc
         join student_info si
              on sc.stu_id = si.stu_id
where sc.course_id in
      (select course_id
       from score_info
       where stu_id = '001' --001的课程
      )
  and sc.stu_id <> '001' --排除001学生
group by si.stu_id, si.stu_name;


-- 炸裂函数
select explode(array(2,3,4)) as item;
select explode(`map`("a",1,"b",2,"c",3)) as (key,value);
select posexplode(array(2,3,4)) as (pos,item);
select inline(`array`(
    named_struct("id",1,"name","zhangsan"),
    named_struct("id",2,"name","lisi"),
    named_struct("id",3,"name","wangwu")));

-- 案例
create table movie_info(
    movie string,     --电影名称
    category string   --电影分类
)
row format delimited fields terminated by "\t";

insert overwrite table movie_info
values ("《疑犯追踪》", "悬疑,动作,科幻,剧情"),
       ("《Lie to me》", "悬疑,警匪,动作,心理,剧情"),
       ("《战狼2》", "战争,动作,灾难");

select * from movie_info;

select
    cate,
    count(*)
from
(select
    movie,
    split(category,",") cates
from movie_info)t1 lateral view explode(cates) tmp as cate
group by cate;

-- 案例
set mapreduce.framework.name=local;
create table order_info
(
    order_id     string, --订单id
    user_id      string, -- 用户id
    user_name    string, -- 用户姓名
    order_date   string, -- 下单日期
    order_amount int     -- 订单金额
);

insert overwrite table order_info
values ('1', '1001', '小元', '2022-01-01', '10'),
       ('2', '1002', '小海', '2022-01-02', '15'),
       ('3', '1001', '小元', '2022-02-03', '23'),
       ('4', '1002', '小海', '2022-01-04', '29'),
       ('5', '1001', '小元', '2022-01-05', '46'),
       ('6', '1001', '小元', '2022-04-06', '42'),
       ('7', '1002', '小海', '2022-01-07', '50'),
       ('8', '1001', '小元', '2022-01-08', '50'),
       ('9', '1003', '小辉', '2022-04-08', '62'),
       ('10', '1003', '小辉', '2022-04-09', '62'),
       ('11', '1004', '小猛', '2022-05-10', '12'),
       ('12', '1003', '小辉', '2022-04-11', '75'),
       ('13', '1004', '小猛', '2022-06-12', '80'),
       ('14', '1003', '小辉', '2022-04-13', '94');

-- 统计每个用户截至每次下单的累积下单总额
select
    order_id,
    user_id,
    user_name,
    order_date,
    order_amount,
    sum(order_amount) over (partition by user_id order by order_date rows between unbounded preceding and current row) sum_so_far
from order_info;


-- 统计每个用户截至每次下单的当月累积下单总额
select
    order_id,
    user_id,
    user_name,
    order_date,
    order_amount,
    sum(order_amount) over (partition by user_id ,month(order_date)  order by order_date rows between unbounded preceding and current row) sum_so_far
from order_info;

-- 统计每个用户每次下单距离上次下单相隔的天数（首次下单按0天算）
select
    order_id,
    user_id,
    user_name,
    order_date,
    order_amount,
    datediff(order_date,last_order_date) diff
from
(select
    order_id,
    user_id,
    user_name,
    order_date,
    order_amount,
    lag(order_date,1,order_date) over (partition by user_id order by order_date) last_order_date
from order_info) t1;

-- 查询所有下单记录以及每个用户的每个下单记录所在月份的首/末次下单日期
-- 方法一
select
    order_id,
    user_id,
    user_name,
    order_date,
    order_amount,
    first_value(order_date,false) over (partition by user_id,month(order_date) order by order_date) first_date,
    first_value(order_date,false) over (partition by user_id,month(order_date) order by order_date desc) last_date
from order_info;

-- 方法二
select
    order_id,
    user_id,
    user_name,
    order_date,
    order_amount,
    first_value(order_date,false) over (partition by user_id,month(order_date) order by order_date) first_date,
    last_value(order_date,false) over (partition by user_id,month(order_date) order by order_date rows between unbounded preceding and unbounded following) last_date
from order_info;


-- 为每个用户的所有下单记录按照订单金额进行排名
select
    order_id,
    user_id,
    user_name,
    order_date,
    order_amount,
    rank() over (partition by user_id order by order_amount desc ) rk,
    dense_rank() over (partition by user_id order by order_amount desc ) drk,
    row_number() over (partition by user_id order by order_amount desc ) rn
from order_info


-- 自定义函数
add jar /opt/module/hive/datas/Hive-Test-1.0-SNAPSHOT.jar;

create temporary function my_len
as "com.atguigu.hive.udf.MyUDF";

show functions like "my_len";

select my_len("abc");

-- 永久函数
create function my_len2
as "com.atguigu.hive.udf.MyUDF"
using jar "hdfs://hadoop102:8020/udf/Hive-Test-1.0-SNAPSHOT.jar";

show functions like "*my_len*";

create database partition_bucket;




