package cn.xiaows.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object StreamingWordCount {

	def main(args: Array[String]): Unit = {
		val conf: SparkConf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[2]")//此处核数须要大于1 一个线程读数据 一个线程处理数据
		val ssc = new StreamingContext(conf, Milliseconds(10000))

		val line: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.31.119", 8888)
		// line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()
		val words: DStream[String] = line.flatMap(_.split(" "))//切分压平
		val wordAndOne: DStream[(String, Int)] = words.map((_, 1))//单词和1组合
		val result: DStream[(String, Int)] = wordAndOne.reduceByKey((a,b) =>{//聚合
			println(s"===========$a _ $b===========")
			a+b
		})
		result.print()

		ssc.start()
		ssc.awaitTermination()
	}

}
