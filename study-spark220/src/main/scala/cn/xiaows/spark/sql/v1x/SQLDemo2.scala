package cn.xiaows.spark.sql.v1x

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SQLDemo2 {

	def main(args: Array[String]): Unit = {
		val conf: SparkConf = new SparkConf().setAppName("SQLDemo2").setMaster("local[2]")
		val sc: SparkContext = new SparkContext(conf)
		val sqlContext = new SQLContext(sc)
		val lines: RDD[String] = sc.textFile("/Users/xiaows/IdeaProjects/BigData/study-spark220/src/main/resources/person.txt")
		val row: RDD[Row] = lines.map(line => {
			val fields: Array[String] = line.split(",")
			val id: Long = fields(0).toLong
			val name = fields(1)
			val age: Int = fields(2).toInt
			val fv: Double = fields(3).toDouble
			Row(id, name, age, fv)
		})
		val schema: StructType = StructType(List(
			StructField("id", LongType, true),
			StructField("name", StringType, true),
			StructField("age", IntegerType, true),
			StructField("fv", DoubleType, true)
		))
		val personDF: DataFrame = sqlContext.createDataFrame(row, schema)
		personDF.registerTempTable("t_person")
		val result: DataFrame = sqlContext.sql("select * from t_person order by fv desc, age asc")
		result.show()
		sc.stop()
	}
}
