CREATE CATALOG batch_test WITH (
'type' = 'hive',
'default-database' = 'dwd',
'hive-conf-dir' = '/Users/renzhuo/IdeaProjects/Flink_test/src/main/resources'
);
create table employee_cost(
    department string,
    pay_month string,
    employee_num bigint,
    cost_sum double,
    primary key(department,pay_month) not enforced
)WITH (
'connector' = 'jdbc',
'url' = 'jdbc:mysql://172.22.17.38:3306/dap',
'table-name' = 'employee_cost',
'driver' = 'com.mysql.cj.jdbc.Driver',
'username' = 'testdap',
'password' = 'h6jtcY_8PepE',
'sink.buffer-flush.max-rows' = '10000',
'sink.buffer-flush.interval' = '200ms',
'sink.max-retries' = '3'
);
insert into employee_cost
select department,pay_month,count(employee_id) as employee_num,sum(cost_sum) as cost_sum
from batch_test.dwd.employee_cost
group by department,pay_month
;