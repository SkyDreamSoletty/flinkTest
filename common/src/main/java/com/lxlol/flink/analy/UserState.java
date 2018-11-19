package com.lxlol.flink.analy;

import lombok.Data;

/**
 * Created by Administrator on 2018/10/30 0030.
 */

@Data
public class UserState {
    private boolean isnew = false;//是否是新来的用户
    private boolean isFisrthour = false;//是否是小时第一次来
    private boolean isFisrtday = false;//是否是今天第一次来
    private boolean isFisrtmonth = false;//是否是这个月第一次来

}
