package cn.xiaows.spark.sql.v2x

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.elasticsearch.spark.rdd.EsSpark

object SQLDemo {
	def main(args: Array[String]): Unit = {

		val session: SparkSession = SparkSession.builder().appName("SQLDemo").master("local[*]").getOrCreate()

		val lines: RDD[String] = session.sparkContext.textFile("study-spark220/src/main/resources/person.txt")
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
		val df: DataFrame = session.createDataFrame(row, schema)
		df.show()
		import session.implicits._
		val df2: Dataset[Row] = df.where($"fv" > 90).filter($"age" >30).orderBy($"fv".desc, $"age".asc)
		df2.show()
		df.write.json("study-spark220/src/main/resources/df")

		// 写入es的两种方式
		/**
		  * 写入es的两种方式
		  * 	1.利用隐式转换,使rdd拥有saveToEs(resource[, cfg])
		  * 	2.调用EsSpark.saveToEs(rdd, resource[, cfg])
		  *
		  */
		// 隐式转换
		import org.elasticsearch.spark._
		lines.saveToEs("index/type")

		EsSpark.saveToEs(lines, "index/type")
		session.stop()
	}

}
