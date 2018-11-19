package xyz.lxlol.flink.stream.reduce;

import com.alibaba.fastjson.JSONObject;
import com.lxlol.flink.analy.PindaoRD;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import xyz.lxlol.flink.stream.task.SSProcessData;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by Administrator on 2018/10/28 0028.
 */
public class PindaoReduce implements ReduceFunction<PindaoRD> {



    @Override
    public PindaoRD reduce(PindaoRD value1, PindaoRD value2) throws Exception {
        PindaoRD pindaoRD = new PindaoRD();
//        System.out.println("value1=="+value1);
//        System.out.println("value2=="+value2);
        pindaoRD.setPingdaoid(value1.getPingdaoid());
        pindaoRD.setCount(value1.getCount()+value2.getCount());
        Long aLong = SSProcessData.map.get(value1.getPingdaoid());
        if(aLong!=null){
            SSProcessData.map.put(value1.getPingdaoid(),aLong+value2.getCount());
        }else{
            SSProcessData.map.put(value1.getPingdaoid(),value1.getCount()+value2.getCount());
        }
        System.out.println("#####_"+ JSONObject.toJSONString(SSProcessData.map));
        return  pindaoRD;
    }
}
