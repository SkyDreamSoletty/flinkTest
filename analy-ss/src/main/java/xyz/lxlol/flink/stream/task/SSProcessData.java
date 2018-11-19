package xyz.lxlol.flink.stream.task;

import com.alibaba.fastjson.JSONObject;
import com.lxlol.flink.analy.PidaoPvUv;
import com.lxlol.flink.analy.PindaoRD;
import com.lxlol.flink.input.KafkaMessage;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import xyz.lxlol.flink.stream.map.PindaoKafkaMap;
import xyz.lxlol.flink.stream.map.PindaopvuvMap;
import xyz.lxlol.flink.stream.reduce.PindaoReduce;
import xyz.lxlol.flink.stream.reduce.PindaopvuvReduce;
import xyz.lxlol.flink.stream.transfer.KafkaMessageSchema;
import xyz.lxlol.flink.stream.transfer.KafkaMessageWatermarks;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SSProcessData {

    public static ConcurrentMap<Long,Long> map = new ConcurrentHashMap<Long, Long>();

    public static void main(String[] args) {
//        System.setProperty("hadoop.home.dir","E:\\soft\\hadoop-2.6.0-cdh5.5.1\\hadoop-2.6.0-cdh5.5.1");
        args = new String[]{"--input-topic","test1","--bootstrap.servers","192.168.0.109:9092",
                "--zookeeper.connect","192.168.0.109:2181","--group.id","myconsumer1","--winsdows.size","10"};

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 5) {
            System.out.println("Missing parameters!\n" +
                    "Usage: Kafka --input-topic <topic>" +
                    "--bootstrap.servers <kafka brokers> " +
                    "--zookeeper.connect <zk quorum> --group.id <some id>");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
        env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//
        FlinkKafkaConsumer010 flinkKafkaConsumer = new FlinkKafkaConsumer010<KafkaMessage>(parameterTool.getRequired("input-topic"), new KafkaMessageSchema(), parameterTool.getProperties());
        DataStream<KafkaMessage> input = env.addSource(flinkKafkaConsumer.assignTimestampsAndWatermarks(new KafkaMessageWatermarks()));
        SingleOutputStreamOperator<PindaoRD> map1 = input.map(new PindaoKafkaMap());
        SingleOutputStreamOperator<PindaoRD> pingdaoid = map1.keyBy("pingdaoid").countWindow(10, 5).reduce(new PindaoReduce());
        pingdaoid.print();
//        DataStream<PidaoPvUv> map = input.flatMap(new PindaopvuvMap());
//        DataStream<PidaoPvUv> reduce = map.keyBy("groupbyfield").countWindow(Long.valueOf(parameterTool.getRequired("winsdows.size"))).reduce(new PindaopvuvReduce());
//        reduce.print();
//        System.out.println("#############___"+JSONObject.toJSONString(reduce));
//        reduce.addSink(new Pindaopvuvsinkreduce()).name("pdpvuvreduce");
        try {
            env.execute("pindaossfx");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
