package flinksql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class _03DemoCDC {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        tenv.executeSql("CREATE TABLE mysql_binlog (" +
                " id INT primary key," +
                " name STRING," +
                " description STRING," +
                " weight DECIMAL(10,3)" +
                ") WITH (" +
                " 'connector' = 'mysql-cdc'," +
                " 'hostname' = 'localhost'," +
                " 'port' = '3306'," +
                " 'username' = 'root'," +
                " 'password' = '123456'," +
                " 'database-name' = 'flink_test'," +
                " 'table-name' = 'people'" +
                ")");

        tenv.executeSql("select * from mysql_binlog").print();
    }
}
