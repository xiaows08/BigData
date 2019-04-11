package cn.xiaows.spark.sql.v2x

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DataSetWordCount {

	def main(args: Array[String]): Unit = {
		val session: SparkSession = SparkSession.builder().appName("SQLWordCount").master("local[2]").getOrCreate()
		// DateSet是一个特殊的DataFrame 只有一列的表 列名默认为value
		val lines: Dataset[String] = session.read.textFile("study-spark220/src/main/resources/wc")

//		val json: DataFrame = session.read.json()
		// 切分压平
		import session.implicits._
		val words: Dataset[String] = lines.flatMap(_.split(" "))

		// 注册临时视图
		words.createTempView("v_wc")
		val res: DataFrame = session.sql("select value, count(*) as counts from v_wc group by value order by counts desc")
//		res.show()
		session.sql("select value, count(1) from v_wc group by value").show()
		session.stop()

	}

}
