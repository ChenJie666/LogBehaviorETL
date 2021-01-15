package com.iotmars.mcook.common;

/**
 * @Description:
 * @Author: CJ
 * @Data: 2021/1/15 11:31
 */
public interface KafkaConstant {

    String BOOTSTRAP_SERVERS = "192.168.32.242:9092,192.168.32.243:9092,192.168.32.244:9092";
    String READ_KAFKA_TOPIC = "ModelLog_Q6";
    String WRITE_SUCCESS_KAFKA_TOPIC = "Log_Q6";
    String WRITE_LATE_KAFKA_TOPIC = "LogLate_Q6";

}
