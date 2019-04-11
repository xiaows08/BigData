package cn.xiaows.spark.sql.v2x

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DSLWordCount {

	def main(args: Array[String]): Unit = {
		val session: SparkSession = SparkSession.builder().appName("SQLWordCount").master("local[2]").getOrCreate()
		// DateSet是一个特殊的DataFrame 只有一列的表 列名默认为value
		val lines: Dataset[String] = session.read.textFile("study-spark220/src/main/resources/wc")

		// 切分压平
		import session.implicits._
		val words: Dataset[String] = lines.flatMap(_.split(" "))

		val counts: DataFrame = words.groupBy($"value" as "word").count().sort($"count".desc)
		counts.show()

		import org.apache.spark.sql.functions._
		val counts2: DataFrame = words.groupBy($"value" as "word").agg(count("*") as "count").orderBy($"count").sort($"count".desc)
		counts2.show()
//		// 注册临时视图
//		words.createTempView("v_wc")
//		val res: DataFrame = session.sql("select value, count(*) as counts from v_wc group by value order by counts desc")
//		res.show()
		session.stop()
	}

}
