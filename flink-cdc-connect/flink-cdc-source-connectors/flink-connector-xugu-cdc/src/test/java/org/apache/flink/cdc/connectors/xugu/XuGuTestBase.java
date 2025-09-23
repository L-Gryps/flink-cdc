package org.apache.flink.cdc.connectors.xugu;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author lps
 * @time 2025/9/17 17:08
 */
public class XuGuTestBase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);

        SourceFunction<String> build = XuGuSource.<String>builder()
                .subscribeName("xugu001")
                .hostname("10.28.25.158")
                .port(5138)
                .username("SYSDBA")
                .password("SYSDBA")
                .databaseName("CDC_TEST")
                .tableList("SYSDBA.XUGU_ALL_TYPE2")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> stringDataStreamSource = env.addSource(build, "xugu-cdc").setParallelism(1);

        stringDataStreamSource.print();
        env.execute();
    }
}
