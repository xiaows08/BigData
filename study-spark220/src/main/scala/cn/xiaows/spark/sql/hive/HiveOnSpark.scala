package cn.xiaows.spark.sql.hive

import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveOnSpark {

	def main(args: Array[String]): Unit = {

		//如果想让hive运行在spark上，一定要开启spark对hive的支持
		val spark = SparkSession.builder()
				.appName("HiveOnSpark")
				.master("local[*]")
				.enableHiveSupport()//启用spark对hive的支持(可以兼容hive的语法了)
				.getOrCreate()

		//想要使用hive的元数据库，必须指定hive元数据的位置，添加一个hive-site.xml到当前程序的classpath下即可

		//有t_boy真个表或试图吗？
//		spark.sql("show tables").show()
		spark.sql("SELECT * FROM app_logs.ext_app_log -- where hm>1700").show()
//		spark.sql("CREATE TABLE niu (id bigint, name string)").show()
		spark.close()
	}
}
