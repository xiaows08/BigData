package cn.xiaows.spark.streaming.kafka

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * kafka 直连消费并自动更新偏移量
  */
object DirectStream {

	def main(args: Array[String]): Unit = {
		val group = "g04"
		val topic = "user"
		val conf = new SparkConf().setAppName("DirectStream").setMaster("local[2]")
		val ssc = new StreamingContext(conf, Seconds(8))

		val kafkaParams = Map[String, Object](
			"bootstrap.servers" -> "kafka-1:9092,kafka-2:9092,kafka-3:9092",
			"key.deserializer" -> classOf[StringDeserializer],
			"value.deserializer" -> classOf[StringDeserializer],
			"group.id" -> group,
			"auto.offset.reset" -> "latest", // earliest,latest
			"enable.auto.commit" -> (false: java.lang.Boolean)
		)

		val topics = Array(topic)
		//在Kafka中记录读取偏移量
		val dstream = KafkaUtils.createDirectStream[String, String](
			ssc,
			LocationStrategies.PreferConsistent,//位置策略
			ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)//订阅的策略
		)

		//迭代DStream中的RDD，将每一个时间点对于的RDD拿出来
		dstream.foreachRDD(kafkaRDD => {
			// val offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges//获取该RDD对于的偏移量
			if (!kafkaRDD.isEmpty()) {
				val lines: RDD[String] = kafkaRDD.map(_.value())
				lines.map(x => {
					val a: String = x
				})
				val res: Array[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect()
				// println(res.toBuffer)
				val conn: Jedis = JedisConnectionPool.getConnection()
				for (m <- res) {// 保存结果到 Redis 中
					val key: String = m._1
					val value: Int = m._2
					if (!conn.exists(key)) {
						conn.set(key, value.toString)
					}
					conn.incrBy(key, value.toLong)
				}
				conn.close()
			}
			println("************")

			// 更新偏移量 some time later, after outputs have completed
			val offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges//获取该RDD对于的偏移量
			dstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
		})

		ssc.start()
		ssc.awaitTermination()
	}
}
