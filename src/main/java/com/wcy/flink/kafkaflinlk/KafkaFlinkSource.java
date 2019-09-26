package com.wcy.flink.kafkaflinlk;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.SimpleOutputFormatOperatorFactory;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * @author wcy
 * @date 2019/9/26 14:32
 * @Description:
 */
public class KafkaFlinkSource {

    public static void main(String[] args) throws Exception{
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment senv = StreamTableEnvironment.create(env);
        //2.kafka
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","111.230.241.253:9092");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("books",new SimpleStringSchema(),properties);
        //消费的偏移量  setStartFromEarliest 从头开始消费
        consumer.setStartFromEarliest();

        DataStreamSource topic = env.addSource(consumer);
        //3.数据转换new MapFunction<String,String>  //第一个参数是kafka传递的类  第二个计算之后返回的类型
        SingleOutputStreamOperator<String> map = topic.map(new MapFunction<String,String>(){
            @Override
            public String map(String book){
                return book;
            };
        });

        //注册内存表
        senv.registerDataStream("books",map,"name");
        //定义sql
        String sql = "select name ,count(1) from books group by name";
        Table result = senv.sqlQuery(sql);
        //重点 回退更新  测试为print
        senv.toRetractStream(result, Row.class).print();

        //实际业务中将计算好的数据写到kafka hdfs mysql redis等

        //提交执行
        env.execute();
    }
}
