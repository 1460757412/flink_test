package org.example;

import org.apache.commons.io.FileUtils;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.io.File;

public class Main {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        String s = FileUtils.readFileToString(new File("src/doc/employee_cost.sql"),"utf-8");
        String[] sqls = s.split(";");
        for (String sql : sqls) {
            tEnv.executeSql(sql);
        }
    }
}