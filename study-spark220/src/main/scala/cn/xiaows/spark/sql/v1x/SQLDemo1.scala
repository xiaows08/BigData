package cn.xiaows.spark.sql.v1x

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SQLDemo1 {

	def main(args: Array[String]): Unit = {
		val conf: SparkConf = new SparkConf().setAppName("SQLDemo1").setMaster("local[2]")
		val sc: SparkContext = new SparkContext(conf)
		val sqlContext = new SQLContext(sc)
		val lines: RDD[String] = sc.textFile("/Users/xiaows/IdeaProjects/BigData/study-spark220/src/main/resources/person.txt")
		val person: RDD[Person] = lines.map(line => {
			val fields: Array[String] = line.split(",")
			val id: Long = fields(0).toLong
			val name = fields(1)
			val age: Int = fields(2).toInt
			val fv: Double = fields(3).toDouble
			Person(id, name, age, fv)
		})
		import sqlContext.implicits._
		val personDF: DataFrame = person.toDF
		personDF.registerTempTable("t_person")
		val result: DataFrame = sqlContext.sql("select * from t_person order by fv desc, age asc")
		result.show()
		sc.stop()
	}
}

case class Person(id: Long, name: String, age: Int, fv: Double)