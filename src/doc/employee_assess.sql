CREATE CATALOG batch_test WITH (
'type' = 'hive',
'default-database' = 'dwd',
'hive-conf-dir' = '/Users/renzhuo/IdeaProjects/Flink_test/src/main/resources'
);
create table employee_assess(
    pay_month string ,
    position_id string,
    position_name string,
    assess_method_name string,
    assess_type_name string,
    pos_cost_sum double ,
    pos_num int,
    pos_cost_avg double
)WITH (
'connector' = 'jdbc',
'url' = 'jdbc:mysql://172.22.17.38:3306/dap',
'table-name' = 'employee_assess',
'driver' = 'com.mysql.cj.jdbc.Driver',
'username' = 'testdap',
'password' = 'h6jtcY_8PepE',
'sink.buffer-flush.max-rows' = '10000',
'sink.buffer-flush.interval' = '200ms',
'sink.max-retries' = '3'
);
insert into employee_assess
select * from batch_test.dwd.employee_assess