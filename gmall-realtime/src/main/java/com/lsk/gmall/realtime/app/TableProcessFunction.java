package com.lsk.gmall.realtime.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lsk.gmall.realtime.bean.TableProcess;
import com.lsk.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    //定义属性,侧输出流标记
    private OutputTag<JSONObject> hbaseTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;


    //定义 Phoenix 的连接
    private Connection connection = null;

    public TableProcessFunction(OutputTag<JSONObject> hbaseTag) {
        this.hbaseTag = hbaseTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化 Phoenix 的连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = (Connection) DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    public TableProcessFunction(OutputTag<JSONObject> hbaseTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.hbaseTag = hbaseTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void processBroadcastElement(String jsonStr, Context context, Collector<JSONObject>
            collector) throws Exception {
        //获取状态
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        //将配置信息流中的数据转换为 JSON 对象 {"database":"", "table":"", "type", "", "data":{"":""}}
        JSONObject jsonObject = JSON.parseObject(jsonStr);
        //取出数据中的表名以及操作类型封装 key
        JSONObject data = jsonObject.getJSONObject("data");
        String table = data.getString("source_table");
        String type = data.getString("operate_type");
        String key = table + ":" + type;
        //取出 Value 数据封装为 TableProcess 对象
        TableProcess tableProcess = JSON.parseObject(data.toString(), TableProcess.class);
        checkTable(tableProcess.getSinkTable(), tableProcess.getSinkColumns(), tableProcess.getSinkPk(), tableProcess.getSinkExtend());
        System.out.println("Key:" + key + "," + tableProcess);
        //广播出去
        broadcastState.put(key, tableProcess);
    }

    /**
     * Phoenix 建表
     *
     * @param sinkTable   表名 test
     * @param sinkColumns 表名字段 id,name,sex
     * @param sinkPk      表主键 id
     * @param sinkExtend  表扩展字段 ""
     *                    create table if not exists mydb.test(id varchar primary key,name
     *                    varchar,sex varchar) ...
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        //给主键以及扩展字段赋默认值
        if (sinkPk == null) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }
        //封装建表 SQL
        StringBuilder createSql = new StringBuilder("create table if not exists")
                .append(GmallConfig.HBASE_SCHEMA).append(".").append(sinkTable).append(" (");
        //遍历添加字段信息
        String[] fields = sinkColumns.split(",");
        for (int i = 0; i < fields.length; i++) {
            //取出字段
            String field = fields[i];
            //判断当前字段是否为主键
            if (sinkPk.equals(field)) {
                createSql.append(field).append(" varchar primary key ");
            } else {
                createSql.append(field).append(" varchar ");
            }
            //如果当前字段不是最后一个字段,则追加","
            if (i < fields.length - 1) {
                createSql.append(",");
            }
        }
        createSql.append(")");
        createSql.append(sinkExtend);
        System.out.println(createSql);
        //执行建表 SQL
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(createSql.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("创建 Phoenix 表" + sinkTable + "失败！");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext,
                               Collector<JSONObject> collector) throws Exception {
        //获取状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        //获取表名和操作类型
        String table = jsonObject.getString("table");
        String type = jsonObject.getString("type");
        String key = table + ":" + type;
        //取出对应的配置信息数据
        TableProcess tableProcess = broadcastState.get(key);
        if (tableProcess != null) {
            //向数据中追加 sink_table 信息
            jsonObject.put("sink_table", tableProcess.getSinkTable());
            //根据配置信息中提供的字段做数据过滤
            filterColumn(jsonObject.getJSONObject("data"), tableProcess.getSinkColumns());
            //判断当前数据应该写往 HBASE 还是 Kafka
            if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                //Kafka 数据,将数据输出到主流
                collector.collect(jsonObject);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                //HBase 数据,将数据输出到侧输出流
                readOnlyContext.output(hbaseTag, jsonObject);
            }
        } else {
            System.out.println("No Key " + key + " In Mysql!");
        }
    }

    //根据配置信息中提供的字段做数据过滤
    private void filterColumn(JSONObject data, String sinkColumns) {
        //保留的数据字段
        String[] fields = sinkColumns.split(",");
        List<String> fieldList = Arrays.asList(fields);
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        // while (iterator.hasNext()) {
        // Map.Entry<String, Object> next = iterator.next();
        // if (!fieldList.contains(next.getKey())) {
        // iterator.remove();
        // }
        // }
        entries.removeIf(next -> !fieldList.contains(next.getKey()));
    }
}
