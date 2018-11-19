package xyz.lxlol.flink.stream.map;

import com.alibaba.fastjson.JSON;
import com.lxlol.flink.analy.PindaoRD;
import com.lxlol.flink.input.KafkaMessage;
import com.lxlol.flink.log.UserscanLog;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2018/10/27 0027.
 */
public class PindaoKafkaMap extends RichMapFunction<KafkaMessage, PindaoRD> {

    @Override
    public PindaoRD map(KafkaMessage value) throws Exception {
        String jsonstring = value.getJsonmessage();
//        System.out.println("map进来的数据=="+jsonstring);
        UserscanLog userscanLog = JSON.parseObject(jsonstring, UserscanLog.class);
        long pingdaoid = userscanLog.getPingdaoid();
        Map<Long,Long> pingdaomap = new HashMap<Long,Long>();
        String mapstring = JSON.toJSONString(pingdaomap);
        PindaoRD pindaoRD = new PindaoRD();
        pindaoRD.setPingdaoid(pingdaoid);
        pindaoRD.setCount(Long.valueOf(value.getCount()+""));
        return pindaoRD;
    }
}
