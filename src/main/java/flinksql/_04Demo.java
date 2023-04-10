package flinksql;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.row;

public class _04Demo {
    public static void main(String[] args) {
        TableEnvironment tenv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tenv.fromValues(DataTypes.ROW(
                DataTypes.FIELD("id",DataTypes.INT()),
                DataTypes.FIELD("name",DataTypes.STRING()),
                DataTypes.FIELD("age",DataTypes.INT())),
                row(1, "zhangsan",18),
                row(2, "lisi",19),
                row(3,"wangwu",20));
    }
}
