package com.iotmars.mcook

import java.util
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.iotmars.mcook.common.KafkaConstant
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

import scala.util.control.Breaks._
import scala.collection.JavaConversions._

/**
 * @Description:
 * @Author: CJ
 * @Data: 2021/1/13 14:04
 */
case class ModelLogQ6(iotId: String, productKey: String, gmtCreate: Long, data: String)

object LogBehaviorEtl {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)

    val readProperties = new Properties()
    readProperties.setProperty("bootstrap.servers", KafkaConstant.BOOTSTRAP_SERVERS)
    readProperties.setProperty("group.id", "consumer-group")
    readProperties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    readProperties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val myConsumer = new FlinkKafkaConsumer[String](KafkaConstant.READ_KAFKA_TOPIC, new SimpleStringSchema(), readProperties)
    //    myConsumer.setStartFromEarliest()
    myConsumer.setStartFromLatest()

    // 从kafka中读取
    val inputStream = env.addSource(myConsumer)
    // 从端口中读取
    //        val inputStream = env.socketTextStream("192.168.32.242", 7777)
    // 从文本中读取
    //        val resource = getClass.getResource("/DeviceModelLog")
    //        val inputStream = env.readTextFile(resource.getPath)

    //    inputStream.print("...")

    val dataStream = inputStream
      .map(log => {
        val jsonObject = JSON.parseObject(log)
        val iotId = jsonObject.getString("iotId")
        val productKey = jsonObject.getString("productKey")
        val gmtCreate = jsonObject.getLong("gmtCreate")
        //        val data = jsonObject.getString("items")
        ModelLogQ6(iotId, productKey, gmtCreate, log)
      })
      //          .assignAscendingTimestamps(_.gmtCreate)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ModelLogQ6](Time.seconds(5)) {
        override def extractTimestamp(element: ModelLogQ6): Long = element.gmtCreate
      })

    val pattern = Pattern
      .begin[ModelLogQ6]("start").where(_.productKey.equals("a17JZbZVctc"))
      .next("next").where(_.productKey.equals("a17JZbZVctc"))

    val outputTag = new OutputTag[String]("order-timeout")

    val selectStream = CEP
      .pattern(dataStream.keyBy(_.iotId), pattern)
      .select(outputTag, new LogTimeoutResult, new LogCompleteResult)

    // 将正确的数据存入Log_Q6中，将迟到的数据存入LogLate_Q6
    val writeProperties = new Properties()
    writeProperties.setProperty("bootstrap.servers", KafkaConstant.BOOTSTRAP_SERVERS)
    //    writeProperties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //    writeProperties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // 迟到数据
    val lateStream = selectStream.getSideOutput(outputTag)
//    lateStream.print("warn")
    lateStream.addSink(new FlinkKafkaProducer[String](KafkaConstant.WRITE_LATE_KAFKA_TOPIC, new SimpleStringSchema(), writeProperties))
    // 正常数据
    val resultStream = selectStream.filter(_ != null)
//    resultStream.print("info")
    resultStream.addSink(new FlinkKafkaProducer[String](KafkaConstant.WRITE_SUCCESS_KAFKA_TOPIC, new SimpleStringSchema(), writeProperties))

    env.execute("Q6 Log ETL")
  }
}

class LogTimeoutResult extends PatternTimeoutFunction[ModelLogQ6, String] {
  override def timeout(map: util.Map[String, util.List[ModelLogQ6]], l: Long): String =
    map.get("start").get(0).data
}

class LogCompleteResult extends PatternSelectFunction[ModelLogQ6, String] {
  override def select(map: util.Map[String, util.List[ModelLogQ6]]): String = {
    val start: ModelLogQ6 = map.get("start").get(0)
    val next: ModelLogQ6 = map.get("next").get(0)
    val oldData: String = start.data
    val newData: String = next.data

    // 解析新数据的事件
    val jsonObject: JSONObject = JSON.parseObject(newData)
    val newItems = jsonObject.getJSONObject("items")
    val keySet: util.Set[String] = newItems.keySet()

    // 解析旧数据的事件
    val oldItems = JSON.parseObject(oldData).getJSONObject("items")

    val changePros = new util.HashMap[String, JSONObject]()

    for (key <- keySet) {
      breakable {
        // 过滤掉目前无用且一直打印的字段
        if ("LFirewallTemp".equals(key) || "StOvRealTemp".equals(key) || "RFirewallTemp".equals(key)) {
          break()
        }

        val oldValue: JSONObject = oldItems.getJSONObject(key)
        val oldValueVa: String = oldValue.getString("value")

        val newValue: JSONObject = newItems.getJSONObject(key)
        val newValueVa: String = newValue.getString("value")

        // 剩余时间取最大值
        if ("StOvSetTimerLeft".equals(key) || "HoodOffLeftTime".equals(key)) {
          if (newValueVa.compareTo(oldValueVa) > 0) {
            changePros.put(key, newValue)
          }
        }

        // 累计运行时间取最大值
        if ("HoodRunningTime".equals(key)) {
          if (newValueVa.compareTo(oldValueVa) < 0) {
            changePros.put(key, newValue)
          }
        }

        if (!oldValueVa.equals(newValueVa)) {
          changePros.put(key, newValue)
        }
      }
    }

    if (changePros.nonEmpty) {
      jsonObject.put("items", changePros)
      jsonObject.toString()
    } else {
      null
    }
  }
}