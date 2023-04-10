package flinksql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class _01Demo {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // 建表（数据源表）
        // {"id":4,"name":"zs","nick":"tiedan","age":18,"gender":"male"}
        tEnv.executeSql(
                "create table t_stu                                 "
                        + " (                                                   "
                        + "   id int ,                                          "  // -- 物理字段
                        + "   name string,                                      "  // -- 物理字段
                        + "   nick string,                                      "
                        + "   age int,                                          "
                        + "   gender string ,                                   "
                        + "   guid as id,                                       "  // -- 表达式字段（逻辑字段）
                        + "   big_age as age + 10 ,                             "  // -- 表达式字段（逻辑字段）
                        + "   offs  bigint metadata from 'offset' ,             "   // -- 元数据字段
                        + "   ts TIMESTAMP_LTZ(3) metadata from 'timestamp'  "   // -- 元数据字段
                        /*+ "   PRIMARY KEY(id,name) NOT ENFORCED                 "*/    // -- 主键约束
                        + " )                                                   "
                        + " WITH (                                              "
                        + "  'connector' = 'kafka',                             "
                        + "  'topic' = 'stu',                              "
                        + "  'properties.bootstrap.servers' = 'hadoop1:9092,hadoop2:9092',   "
                        + "  'properties.group.id' = 'g1',                      "
                        + "  'scan.startup.mode' = 'earliest-offset',           "
                        + "  'format' = 'json',                                 "
                        + "  'json.fail-on-missing-field' = 'false',            "
                        + "  'json.ignore-parse-errors' = 'true'                "
                        + " )                                                   "
        );

        tEnv.executeSql("select * from t_stu").print();
    }
}
