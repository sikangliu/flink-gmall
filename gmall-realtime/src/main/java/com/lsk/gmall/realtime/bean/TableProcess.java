package com.lsk.gmall.realtime.bean;

import lombok.Data;

/**
 * @Description
 * @Author sikang.liu
 * @Date 2022-07-27 18:43
 */
@Data
public class TableProcess {
    //动态分流 Sink 常量
    public static final String SINK_TYPE_HBASE = "hbase";
    public static final String SINK_TYPE_KAFKA = "kafka";
    public static final String SINK_TYPE_CK = "clickhouse";
    //来源表
    String sourceTable;
    //操作类型 insert,update,delete
    String operateType;
    //输出类型 hbase kafka
    String sinkType;
    //输出表(主题)
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段
    String sinkPk;
    //建表扩展
    String sinkExtend;
}
