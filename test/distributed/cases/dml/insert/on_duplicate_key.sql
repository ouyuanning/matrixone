create table t1(a int primary key, b int);
delete from t1;
insert into t1 values (1,1);
insert into t1 values (1,2), (2,2) on duplicate key update b=b+10;
select * from t1;
delete from t1;
insert into t1 values (1,1);
insert into t1 values (1,2), (2,2) on duplicate key update b=values(b)+10;
select * from t1;
delete from t1;
insert into t1 values (1,1);
insert into t1 values (1,11), (2,22), (3,33) on duplicate key update a=a+1,b=100;
-- @bvt:issue#4423
select * from t1;
-- @bvt:issue
delete from t1;
insert into t1 values (1,1);
insert into t1 values (1,2), (1,22) on duplicate key update b=b+10;
select * from t1;
delete from t1;
insert into t1 values (1,1),(3,3);
insert into t1 values (1,2),(2,22) on duplicate key update a=a+1;
delete from t1;
insert into t1 values (1,1),(3,3);
insert into t1 values (1,2),(2,22),(3,33) on duplicate key update a=a+1;
drop table t1;
create table t1(a int, b int, c int, primary key(a, b));
delete from t1;
insert into t1 values (1,1,1);
insert into t1 values (1,1,2), (2,2,2) on duplicate key update c=c+10;
select * from t1;
delete from t1;
insert into t1 values (1,1,1);
insert into t1 values (1,1,2), (2,2,2) on duplicate key update c=values(c)+10;
select * from t1;
drop table t1;
create table t1(a int, b int, c int, primary key(a, b), key(c));
delete from t1;
insert into t1 values (1,1,1);
insert into t1 values (1,1,2), (2,2,2) on duplicate key update c=c+10;
select * from t1;
delete from t1;
insert into t1 values (1,1,1);
insert into t1 values (1,1,2), (2,2,2) on duplicate key update c=values(c)+10;
select * from t1;
drop table t1;
create table t1(a int, b int, c int, primary key(a, b), key(b, c));
delete from t1;
insert into t1 values (1,1,1);
insert into t1 values (1,1,2), (2,2,2), (2,2,3) on duplicate key update c=c+10;
select * from t1;
delete from t1;
insert into t1 values (1,1,1);
insert into t1 values (1,1,2), (2,2,2), (2,2,3) on duplicate key update c=values(c)+10;
select * from t1;
drop table t1;
create table t1(a int unique key, b int);
delete from t1;
insert into t1 values (1,1);
insert into t1 values (1,2), (2,2) on duplicate key update b=b+10;
select * from t1;
delete from t1;
insert into t1 values (1,1);
insert into t1 values (1,2), (2,2) on duplicate key update b=values(b)+10;
select * from t1;
delete from t1;
insert into t1 values (1,1);
insert into t1 values (1,11), (2,22), (3,33) on duplicate key update a=a+1,b=100;
-- @bvt:issue#4423
select * from t1;
-- @bvt:issue
delete from t1;
insert into t1 values (1,1);
insert into t1 values (1,2), (1,22) on duplicate key update b=b+10;
select * from t1;
delete from t1;
insert into t1 values (1,1),(3,3);
insert into t1 values (1,2),(2,22) on duplicate key update a=a+1;
delete from t1;
insert into t1 values (1,1),(3,3);
insert into t1 values (1,2),(2,22),(3,33) on duplicate key update a=a+1;
drop table t1;
create table t1(a int, b int, c int, unique key(a, b));
delete from t1;
insert into t1 values (1,1,1);
insert into t1 values (1,1,2), (2,2,2) on duplicate key update c=c+10;
select * from t1;
delete from t1;
insert into t1 values (1,1,1);
insert into t1 values (1,1,2), (2,2,2) on duplicate key update c=values(c)+10;
select * from t1;
drop table t1;
CREATE TABLE IF NOT EXISTS indup_00(`id` INT UNSIGNED,`act_name` VARCHAR(20) NOT NULL,`spu_id` VARCHAR(30) NOT NULL,`uv`  BIGINT NOT NULL,`update_time` date default '2020-10-10' COMMENT 'lastest time',unique key idx_act_name_spu_id (act_name,spu_id));
insert into indup_00 values (1,'beijing','001',1,'2021-01-03'),(2,'shanghai','002',2,'2022-09-23'),(3,'guangzhou','003',3,'2022-09-23');
select * from indup_00 order by id;
insert into indup_00 values (6,'shanghai','002',21,'1999-09-23'),(7,'guangzhou','003',31,'1999-09-23') on duplicate key update `act_name`=VALUES(`act_name`), `spu_id`=VALUES(`spu_id`), `uv`=VALUES(`uv`);
-- @bvt:issue#4423
select * from indup_00 order by id;
-- @bvt:issue
drop table indup_00;
CREATE TABLE IF NOT EXISTS indup(
col1 INT primary key,
col2 VARCHAR(20) NOT NULL,
col3 VARCHAR(30) NOT NULL,
col4 BIGINT default 30
);
insert into indup values(22,'11','33',1), (23,'22','55',2),(24,'66','77',1),(25,'99','88',1),(22,'11','33',1) on duplicate key update col1=col1+col2;
-- @bvt:issue#4423
select * from indup;
insert into indup values(24,'1','1',100) on duplicate key update col1=2147483649;
select * from indup;
-- @bvt:issue
drop table indup;
create table t1(a int primary key, b int, c int);
insert into t1 values (1,1,1),(2,2,2);
insert into t1 values (1,9,1),(11,8,2) on duplicate key update a=a+10, c=10;
-- @bvt:issue#4423
select * from t1 order by a;
-- @bvt:issue

drop table if exists t1;
create table t1(a int primary key, b int unique key);
insert into t1 values (1,1),(2,2),(3,3);
insert into t1 values (1,20) on duplicate key update b = b + 1;
insert into t1 values (20,1) on duplicate key update a = a + 1;
delete from t1;
insert into t1 values (1,1),(3,2);
insert into t1 values (1,2) on duplicate key update a = 10;
delete from t1;
insert into t1 values (1,1),(3,2);
insert into t1 values (1,2) on duplicate key update a = a+2;

-- @bvt:issue#16438
drop table if exists t1;
create table t1(a int primary key, b int) partition by key(a) partitions 2;
insert into t1 values (1,1),(2,2);
insert into t1 values (1,1),(3,3) on duplicate key update b = 10;
select * from t1 order by a;
drop table if exists t1;
create table t1(a int, b int, c int, primary key(a,b)) partition by key(a,b) partitions 2;
insert into t1 values (1,1,1),(2,2,2);
insert into t1 values (1,1,1),(3,3,3) on duplicate key update c = 10;
select * from t1 order by a;
-- @bvt:issue
drop table if exists t1;
create table t1(a int primary key, b int);
insert into t1 values (1,1),(2,2);
prepare s1 from insert into t1 values (?,2) on duplicate key update b = 10;
set @a=1;
execute s1 using @a;
execute s1 using @a;
execute s1 using @a;
execute s1 using @a;

drop table if exists users;
create table users (id int primary key auto_increment, counter int, create_at datetime default current_timestamp, update_at datetime default current_timestamp on update current_timestamp);
insert into users (id, counter) values ('112',1);
select id, counter, create_at = update_at from users;
select sleep(1);
insert into users (id, counter) values ('112',2) on duplicate key update counter=counter+values(counter), create_at=current_timestamp();
select id, counter, create_at = update_at from users;
