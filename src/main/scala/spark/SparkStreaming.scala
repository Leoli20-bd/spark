package spark

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import tools.RedisClientUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent


object SparkStreaming {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("sparkStreaming")
      .master("local[2]")
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    val topic = "t1"
    val groupId = "g1"
    val bootstrap = "hadoop1:9092,hadoop2:90092"
    val maxpoll = 2000
    val jedis = RedisClientUtils.getJedis
    val partitions = 6
    var fromOffset: collection.Map[TopicPartition, Long] = Map[TopicPartition, Long]()
    for (partition <- 0 until (partitions)) {
      val topicPartition = new TopicPartition(topic, partition)
      val offset = jedis.get(s"${topic}_${partition}").toLong
      fromOffset += (topicPartition -> offset)
    }

    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> maxpoll,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      ConsumerStrategies.Assign[String, String](fromOffset.keys, kafkaParams, fromOffset)
    )

    stream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      if (!rdd.isEmpty()) {
        val jedis = RedisClientUtils.getJedis
        val p = jedis.pipelined()
        p.multi() //开启事务
        //处理数据
        rdd.map(_.value()).map(x => {

        })
        //更新offset
        offsetRanges.foreach(offsetRange => {
          val topic = offsetRange.topic
          val partition = offsetRange.partition
          val fromOffset = offsetRange.fromOffset
          val untilOffset = offsetRange.untilOffset
          p.set(topic + "_" + partition, untilOffset + "")
        })
        //提交事务
        p.exec()
        //关闭事务
        p.sync()
        jedis.close()
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
