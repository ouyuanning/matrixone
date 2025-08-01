drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

-- @ignore:2,3,4,5,6,7,8,9
show accounts;

-- @session:id=1&user=acc01:test_account&password=111
drop database if exists db01;
create database db01;
use db01;
create table t01 (a int);
insert into t01 values (1);
-- @session

drop snapshot if exists snapshot_acc01_dropped;
create snapshot snapshot_acc01_dropped for account acc01;

drop account if exists acc01;

select * from db01.t01{snapshot='snapshot_acc01_dropped'};

restore account acc01 from snapshot snapshot_acc01_dropped;

-- @session:id=2&user=acc01:test_account&password=111
select * from db01.t01;
-- @session

-- @ignore:2,3,4,5,6,7,8,9
show accounts;

drop account if exists acc01;
drop snapshot if exists snapshot_acc01_dropped;


drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

-- @ignore:2,3,4,5,6,7,8,9
show accounts;

-- @session:id=3&user=acc01:test_account&password=111
drop database if exists db01;
create database db01;
use db01;
create table t01 (a int);
insert into t01 values (1);
-- @session

drop snapshot if exists snapshot_acc01_dropped_created;
create snapshot snapshot_acc01_dropped_created for account acc01;

drop account if exists acc01;

create account acc01 admin_name = 'test_account' identified by '123456';

restore account acc01 from snapshot snapshot_acc01_dropped_created;

-- @session:id=4&user=acc01:test_account&password=111
select * from db01.t01;
-- @session

-- @ignore:2,3,4,5,6,7,8,9
show accounts;

drop account if exists acc01;
drop snapshot if exists snapshot_acc01_dropped_created;
