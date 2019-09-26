package com.wcy.flink.kafkaflinlk;

import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * @author wcy
 * @date 2019/9/26 15:50
 * @Description:
 */
public class KafkaStart {
    public static void main(String[] args){
        ConfigurableApplicationContext context = SpringApplication.run(KafkaStart.class, args);
        KafkaSender sender = context.getBean(KafkaSender.class);
        for (int i = 0; i < 300; i++) {
            //调用消息发送类中的消息发送方法
            sender.send();

            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
