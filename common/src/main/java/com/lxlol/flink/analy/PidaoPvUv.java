package com.lxlol.flink.analy;

import lombok.Data;

@Data
public class PidaoPvUv {

    private long pingdaoid;
    private long userid;
    private long pvcount;
    private long uvcount;
    private long timestamp;
    private String timestring;
    private String groupbyfield;

}
