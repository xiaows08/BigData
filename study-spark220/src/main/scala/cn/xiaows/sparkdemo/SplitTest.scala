package cn.xiaows.sparkdemo

import java.util.Arrays

object SplitTest {
	def main(args: Array[String]): Unit = {
		val line = "http://bigdata.edu360.cn/laozhao"

		val splits: Array[String] = line.split("/")

//		学科 老师
		val subject: String = splits(2).split("[.]")(0)
		val teacher: String = splits(3)
		println(subject, teacher)
	}

}
