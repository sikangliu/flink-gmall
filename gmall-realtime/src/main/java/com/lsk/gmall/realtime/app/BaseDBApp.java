package com.lsk.gmall.realtime.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.lsk.gmall.realtime.bean.TableProcess;
import com.lsk.gmall.realtime.utils.MyKafkaUtil;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.lang.Nullable;

/**
 * @Description
 * @Author sikang.liu
 * @Date 2022-07-27 18:34
 */
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //1.1 设置状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));
        //1.2 开启 CK
        //env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000L);

        //2.读取 Kafka 数据
        String topic = "ods_base_db_";
        String groupId = "ods_db_group";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //3.将每行数据转换为 JSON 对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);
        //4.过滤
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        //获取 data 字段
                        String data = value.getString("data");
                        return data != null && data.length() > 0;
                    }
                });
        //打印测试
        filterDS.print();

        //5.创建 MySQL CDC Source
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall2021-realtime")
                .tableList("gmall2021-realtime.table_process")
                .deserializer(new DebeziumDeserializationSchema<String>() {
                    //反序列化方法
                    @Override
                    public void deserialize(SourceRecord sourceRecord, Collector<String> collector)
                            throws Exception {
                        //库名&表名
                        String topic = sourceRecord.topic();
                        String[] split = topic.split("\\.");
                        String db = split[1];
                        String table = split[2];
                        //获取数据
                        Struct value = (Struct) sourceRecord.value();

                        Struct after = value.getStruct("after");
                        JSONObject data = new JSONObject();
                        if (after != null) {
                            Schema schema = after.schema();
                            for (Field field : schema.fields()) {
                                data.put(field.name(), after.get(field.name()));
                            }
                        }
                        //获取操作类型
                        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
                        //创建 JSON 用于存放最终的结果
                        JSONObject result = new JSONObject();
                        result.put("database", db);
                        result.put("table", table);
                        result.put("type", operation.toString().toLowerCase());
                        result.put("data", data);
                        collector.collect(result.toJSONString());
                    }

                    //定义数据类型
                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();

        //6.读取 MySQL 数据
        DataStreamSource<String> tableProcessDS = env.addSource(sourceFunction);
        //7.将配置信息流作为广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new
                MapStateDescriptor<>("table-process-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessDS.broadcast(mapStateDescriptor);
        //8.将主流和广播流进行链接
        BroadcastConnectedStream<JSONObject, String> connectedStream = filterDS.connect(broadcastStream);

        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE) {
        };
        SingleOutputStreamOperator<JSONObject> kafkaJsonDS = connectedStream.process(new
                TableProcessFunction(hbaseTag));
        DataStream<JSONObject> hbaseJsonDS = kafkaJsonDS.getSideOutput(hbaseTag);

        hbaseJsonDS.addSink(new DimSink());

        FlinkKafkaProducer<JSONObject> kafkaSinkBySchema = MyKafkaUtil.getKafkaSinkBySchema(
                new KafkaSerializationSchema<JSONObject>() {
                    @Override
                    public void open(SerializationSchema.InitializationContext context) throws Exception {
                        System.out.println("开始序列化 Kafka 数据！");
                    }

                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                        return new ProducerRecord<byte[], byte[]>(element.getString("sink_table"),
                                element.getString("data").getBytes());
                    }
                });
        kafkaJsonDS.addSink(kafkaSinkBySchema);
        //7.执行任务
        env.execute();
    }
}
