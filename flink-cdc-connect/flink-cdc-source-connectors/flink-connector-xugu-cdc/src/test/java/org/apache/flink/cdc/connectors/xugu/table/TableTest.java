package org.apache.flink.cdc.connectors.xugu.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author lps
 * @time 2025/9/25 17:42
 */
public class TableTest {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());

        String sourceDDL =
                String.format(
                        "CREATE TABLE XUGU_ALL_TYPE2_SOURCE(\n"
                                + "ID INT NOT NULL,\n"
                                + "TINYINT_COL TINYINT,\n"
                                + "SMALLINT_COL SMALLINT,\n"
                                + "INTEGER_COL INT,\n"
                                + "BIGINT_COL BIGINT,\n"
                                + "FLOAT_COL FLOAT,\n"
                                + "DOUBLE_COL DOUBLE,\n"
                                + "NUMERIC_COL DECIMAL(10,5),\n"
                                + "BOOL_COL BOOLEAN,\n"
                                + "CHAR_COL STRING,\n"
                                + "VARCHAR_COL STRING,\n"
                                + "CLOB_COL STRING,\n"
                                + "BLOB_COL BYTES,\n"
                                + "BINARY_COL BYTES,\n"
                                + "DATE_COL DATE,\n"
                                + "TIME_COL TIME,\n"
                                + "TIMESTAMP_COL TIMESTAMP, \n"
                                + "PRIMARY KEY (ID) NOT ENFORCED \n"
                                + ") WITH (\n"
                                + " 'connector' = 'xugu-cdc', \n"
                                + " 'hostname' = '%s', \n"
                                + " 'port' = '%s', \n"
                                + " 'username' = '%s', \n"
                                + " 'password' = '%s', \n"
                                + " 'database-name' = '%s', \n"
                                + " 'schema-name' = '%s', \n"
                                + " 'table-name' = '%s', \n"
                                + " 'subscribe-name' = '%s', \n"
                                + " 'server-time-zone'='%s' \n"
                                + ")",
                        "10.28.25.158",
                        5138,
                        "SYSDBA",
                        "SYSDBA",
                        "CDC_TEST",
                        "SYSDBA",
                        "XUGU_ALL_TYPE2",
                        "xugu001",
                        "UTC+8");

        tEnv.executeSql(sourceDDL);
        TableResult result = tEnv.executeSql("SELECT * FROM XUGU_ALL_TYPE2_SOURCE");
        result.print();
    }

    private static List<String> fetchRows(Iterator<Row> iter, int size) {
        List<String> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            Row row = iter.next();
            rows.add(row.toString());
            size--;
        }
        return rows;
    }
}
