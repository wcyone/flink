package com.wcy.flink.kafkaflinlk;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * @author wcy
 * @date 2019/8/14 15:32
 * @Description:
 */
@Data
public class Message implements Serializable {
    private Long id;    //id

    private String msg; //消息

    private String sendTime;  //时间戳
}
