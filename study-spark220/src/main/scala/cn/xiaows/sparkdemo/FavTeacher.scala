package cn.xiaows.sparkdemo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FavTeacher {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("FavTeacher").setMaster("local[4]")
		val sc: SparkContext = new SparkContext(conf)
		val lines: RDD[String] = sc.textFile(args(0))

	}

}
