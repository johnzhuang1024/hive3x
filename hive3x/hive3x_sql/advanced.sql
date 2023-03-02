set mapreduce.framework.name=local;

-- 同时在线人数问题
-- 建表
drop table if exists live_events;
create table if not exists live_events
(
    user_id      int comment '用户id',
    live_id      int comment '直播id',
    in_datetime  string comment '进入直播间时间',
    out_datetime string comment '离开直播间时间'
)
    comment '直播间访问记录';
-- 数据装载
INSERT overwrite table live_events
VALUES (100, 1, '2021-12-01 19:00:00', '2021-12-01 19:28:00'),
       (100, 1, '2021-12-01 19:30:00', '2021-12-01 19:53:00'),
       (100, 2, '2021-12-01 21:01:00', '2021-12-01 22:00:00'),
       (101, 1, '2021-12-01 19:05:00', '2021-12-01 20:55:00'),
       (101, 2, '2021-12-01 21:05:00', '2021-12-01 21:58:00'),
       (102, 1, '2021-12-01 19:10:00', '2021-12-01 19:25:00'),
       (102, 2, '2021-12-01 19:55:00', '2021-12-01 21:00:00'),
       (102, 3, '2021-12-01 21:05:00', '2021-12-01 22:05:00'),
       (104, 1, '2021-12-01 19:00:00', '2021-12-01 20:59:00'),
       (104, 2, '2021-12-01 21:57:00', '2021-12-01 22:56:00'),
       (105, 2, '2021-12-01 19:10:00', '2021-12-01 19:18:00'),
       (106, 3, '2021-12-01 19:01:00', '2021-12-01 21:10:00');

-- 代码实现
select t2.live_id,
       max(t2.sum_event_datetime) max_event_datetime
from (select t1.user_id,
             t1.live_id,
             t1.event_datetime,
             sum(t1.flag) over (partition by live_id order by event_datetime) sum_event_datetime
      from (select user_id,
                   live_id,
                   in_datetime event_datetime,
                   1           flag
            from live_events
            union all
            select user_id,
                   live_id,
                   out_datetime event_datetime,
                   -1           flag
            from live_events) t1) t2
group by live_id;


-- 会话划分问题
-- 建表语句
drop table if exists page_view_events;
create table if not exists page_view_events
(
    user_id        int comment '用户id',
    page_id        string comment '页面id',
    view_timestamp bigint comment '访问时间戳'
)
    comment '页面访问记录';

-- 数据装载
insert overwrite table page_view_events
values (100, 'home', 1659950435),
       (100, 'good_search', 1659950446),
       (100, 'good_list', 1659950457),
       (100, 'home', 1659950541),
       (100, 'good_detail', 1659950552),
       (100, 'cart', 1659950563),
       (101, 'home', 1659950435),
       (101, 'good_search', 1659950446),
       (101, 'good_list', 1659950457),
       (101, 'home', 1659950541),
       (101, 'good_detail', 1659950552),
       (101, 'cart', 1659950563),
       (102, 'home', 1659950435),
       (102, 'good_search', 1659950446),
       (102, 'good_list', 1659950457),
       (103, 'home', 1659950541),
       (103, 'good_detail', 1659950552),
       (103, 'cart', 1659950563);

-- 代码实现
select user_id,
       page_id,
       view_timestamp,
       concat(user_id, "-", sum(flag)
                                over (partition by user_id order by view_timestamp rows between unbounded preceding and current row )) seesion_id
from (select user_id,
             page_id,
             view_timestamp,
             mid_time,
             `if`(mid_time = 0 or mid_time >= 60, 1, 0) flag
      from (select user_id,
                   page_id,
                   view_timestamp,
                   view_timestamp -
                   lag(view_timestamp, 1, view_timestamp) over (partition by user_id order by view_timestamp) mid_time
            from page_view_events) t1) t2;

-- 间断连续登录用户问题
-- 建表语句
drop table if exists login_events;
create table if not exists login_events
(
    user_id        int comment '用户id',
    login_datetime string comment '登录时间'
)
    comment '直播间访问记录';
-- 数据装载
INSERT overwrite table login_events
VALUES (100, '2021-12-01 19:00:00'),
       (100, '2021-12-01 19:30:00'),
       (100, '2021-12-02 21:01:00'),
       (100, '2021-12-03 11:01:00'),
       (101, '2021-12-01 19:05:00'),
       (101, '2021-12-01 21:05:00'),
       (101, '2021-12-03 21:05:00'),
       (101, '2021-12-05 15:05:00'),
       (101, '2021-12-06 19:05:00'),
       (102, '2021-12-01 19:55:00'),
       (102, '2021-12-01 21:05:00'),
       (102, '2021-12-02 21:57:00'),
       (102, '2021-12-03 19:10:00'),
       (104, '2021-12-04 21:57:00'),
       (104, '2021-12-02 22:57:00'),
       (105, '2021-12-01 10:01:00');

-- 代码实现
select user_id,
       sum(flag) over (partition by user_id order by daytime)
from (select user_id,
             daytime,
             mid_login_datetime,
             if(mid_login_datetime = 0 or mid_login_datetime > 2, 0, mid_login_datetime) flag
      from (select distinct user_id,
                            date_format(login_datetime, "yyyy-MM-dd")                                         daytime,
                            datediff(login_datetime, lag(login_datetime, 1, login_datetime)
                                                         over (partition by user_id order by login_datetime)) mid_login_datetime
            from login_events) t1) t2;



select user_id,
       sum(mid_login_datetime)+1 max_day_count
from
(select user_id,
       daytime,
       mid_login_datetime,
       flag,
       sum(flag) over (partition by user_id order by daytime) sum_flag
from (select user_id,
             daytime,
             mid_login_datetime,
             if(mid_login_datetime = 0 or mid_login_datetime > 2, 1, 0) flag
      from (select distinct user_id,
                            date_format(login_datetime, "yyyy-MM-dd")                                         daytime,
                            datediff(login_datetime, lag(login_datetime, 1, login_datetime)
                                                         over (partition by user_id order by login_datetime)) mid_login_datetime
            from login_events)t1) t2) t3
group by t3.user_id,sum_flag;


-- 日期交叉问题
-- 建表语句
drop table if exists promotion_info;
create table promotion_info
(
    promotion_id string comment '优惠活动id',
    brand        string comment '优惠品牌',
    start_date   string comment '优惠活动开始日期',
    end_date     string comment '优惠活动结束日期'
) comment '各品牌活动周期表';
-- 数据装载
insert overwrite table promotion_info
values (1, 'oppo', '2021-06-05', '2021-06-09'),
       (2, 'oppo', '2021-06-11', '2021-06-21'),
       (3, 'vivo', '2021-06-05', '2021-06-15'),
       (4, 'vivo', '2021-06-09', '2021-06-21'),
       (5, 'redmi', '2021-06-05', '2021-06-21'),
       (6, 'redmi', '2021-06-09', '2021-06-15'),
       (7, 'redmi', '2021-06-17', '2021-06-26'),
       (8, 'huawei', '2021-06-05', '2021-06-26'),
       (9, 'huawei', '2021-06-09', '2021-06-15'),
       (10, 'huawei', '2021-06-17', '2021-06-21');

-- 代码实现
select brand,
       sum(if(last_flag=0,1,datediff(event_date,last_date))) promotion_day_count
from
(select brand,
       event_date,
       sum_promotion_date,
       lag(event_date,1,event_date) over (partition by brand order by event_date) last_date,
       lag(sum_promotion_date,1,0) over (partition by brand order by event_date) last_flag
from
(select brand,
       event_date,
       sum(flag) over (partition by brand order by event_date) sum_promotion_date
from
(select promotion_id,
       brand,
       start_date event_date,
       1 flag
from promotion_info
union all
select promotion_id,
       brand,
       end_date event_date,
       -1 flag
from promotion_info) t1) t2) t3
group by brand;