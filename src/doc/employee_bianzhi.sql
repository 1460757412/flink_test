CREATE CATALOG batch_test WITH (
'type' = 'hive',
'default-database' = 'dwd',
'hive-conf-dir' = '/Users/renzhuo/IdeaProjects/Flink_test/src/main/resources'
);
create table employee_bianzhi(
position_id string ,
position_name string,
job_group string,
job_family_name string,
job_type_chinese string,
iskeytalent string,
ismanagement string ,
department string ,
department_level int ,
department_fl string ,
level1 string ,
level2 string ,
level3 string ,
level4 string ,
level5 string ,
headcount int ,
onduty_cnt int ,
rate double
)WITH (
'connector' = 'jdbc',
'url' = 'jdbc:mysql://172.22.17.38:3306/dap',
'table-name' = 'employee_bianzhi',
'driver' = 'com.mysql.cj.jdbc.Driver',
'username' = 'testdap',
'password' = 'h6jtcY_8PepE',
'sink.buffer-flush.max-rows' = '10000',
'sink.buffer-flush.interval' = '200ms',
'sink.max-retries' = '3'
);
insert into employee_bianzhi
select * from batch_test.dwd.employee_bianzhi