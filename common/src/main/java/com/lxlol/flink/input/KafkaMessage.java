package com.lxlol.flink.input;

import lombok.Data;

/**
 * Created by Administrator on 2018/10/27 0027.
 */
@Data
public class KafkaMessage {
    private String jsonmessage;//json格式的消息内容
    private int count;//消息的次数
    private Long timestamp;//消息的时间

}
