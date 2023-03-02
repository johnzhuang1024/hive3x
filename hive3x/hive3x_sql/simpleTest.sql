-- 创建数据库
create database beginner;
set mapreduce.framework.name=local;

-- 创建学生表
DROP TABLE IF EXISTS student;
create table if not exists student_info
(
    stu_id   string COMMENT '学生id',
    stu_name string COMMENT '学生姓名',
    birthday string COMMENT '出生日期',
    sex      string COMMENT '性别'
)
    row format delimited fields terminated by ','
    stored as textfile;

-- 创建课程表
DROP TABLE IF EXISTS course;
create table if not exists course_info
(
    course_id   string COMMENT '课程id',
    course_name string COMMENT '课程名',
    tea_id      string COMMENT '任课老师id'
)
    row format delimited fields terminated by ','
    stored as textfile;

-- 创建老师表
DROP TABLE IF EXISTS teacher;
create table if not exists teacher_info
(
    tea_id   string COMMENT '老师id',
    tea_name string COMMENT '老师姓名'
)
    row format delimited fields terminated by ','
    stored as textfile;

-- 创建分数表
DROP TABLE IF EXISTS score;
create table if not exists score_info
(
    stu_id    string COMMENT '学生id',
    course_id string COMMENT '课程id',
    score     int COMMENT '成绩'
)
    row format delimited fields terminated by ','
    stored as textfile;

-- 加载数据
load data local inpath '/opt/module/data/student_info.txt' into table student_info;
load data local inpath '/opt/module/data/course_info.txt' into table course_info;
load data local inpath '/opt/module/data/teacher_info.txt' into table teacher_info;
load data local inpath '/opt/module/data/score_info.txt' into table score_info;


-- 题目
-- 2.1.1 查询姓名中带“冰”的学生名单
select *
from student_info
where stu_name like "%冰%";

-- 2.1.3 检索课程编号为“04”且分数小于60的学生的课程信息，结果按分数降序排列
select *
from student_info
         join score_info
              on student_info.stu_id = score_info.stu_id
where course_id = "04"
  and score < 60
order by score desc;

-- 2.1.4 查询数学成绩不及格的学生和其对应的成绩，按照学号升序排序
select student_info.stu_id id,
       stu_name            name,
       score
from student_info
         join
     (select *
      from course_info
               join score_info
                    on score_info.course_id = course_info.course_id
      where course_name = "数学"
        and score < 60) t1
     on t1.stu_id = student_info.stu_id
order by id asc;

-- 3.1.1 查询编号为“02”的课程的总成绩
select course_id,
       sum(score) score
from score_info
where course_id = "02"
group by course_id;

-- 3.2.1 查询各科成绩最高和最低的分，以如下的形式显示：课程号，最高分，最低分
select course_id,
       max(score) max_score,
       min(score) min_score
from score_info
group by course_id;

-- 3.2.2 查询每门课程有多少学生参加了考试（有考试成绩）
select course_id,
       count(course_id) stu_num
from score_info
group by course_id;

-- 3.4.2 按照如下格式显示学生的语文、数学、英语三科成绩，没有成绩的输出为0，按照学生的有效平均成绩降序显示
-- 学生id 语文 数学 英语 有效课程数 有效平均成绩
select stu_id,
       sum(if(ci.course_name = '语文', score, 0)) `语文`,
       sum(if(ci.course_name = '数学', score, 0)) `数学`,
       sum(if(ci.course_name = '英语', score, 0)) `英语`,
       count(*)                                   `有效课程数`,
       avg(score)                              `平均成绩`
from score_info s1 join course_info ci
on s1.course_id = ci.course_id
group by stu_id
order by `平均成绩` desc;




