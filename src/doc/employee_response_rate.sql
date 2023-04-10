CREATE CATALOG batch_test WITH (
'type' = 'hive',
'default-database' = 'dwd',
'hive-conf-dir' = '/Users/renzhuo/IdeaProjects/Flink_test/src/main/resources'
);
create table employee_response_rate(
    pay_month date ,
    department string ,
    callcenter_num int ,
    callcenter_cost double ,
    level3_unit_id string ,
    employee_num int ,
    cost_sum double ,
    department_fl string ,
    flight_count int ,
    flight_hours double ,
    nkg_flight_hours double ,
    total_turnover double ,
    total_callinanswer double ,
    roi double
)WITH (
'connector' = 'jdbc',
'url' = 'jdbc:mysql://172.22.17.38:3306/dap',
'table-name' = 'employee_response_rate',
'driver' = 'com.mysql.cj.jdbc.Driver',
'username' = 'testdap',
'password' = 'h6jtcY_8PepE',
'sink.buffer-flush.max-rows' = '10000',
'sink.buffer-flush.interval' = '200ms',
'sink.max-retries' = '3'
);
insert into employee_response_rate
select *
from batch_test.dwd.employee_response_rate
;