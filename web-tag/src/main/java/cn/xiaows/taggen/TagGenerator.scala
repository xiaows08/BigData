package cn.xiaows.taggen

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TagGenerator {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("tagGen")
        conf.setMaster("local[4]")
        val sc = new SparkContext(conf)

        val poi_tags = sc.textFile("/Users/xiaows/IdeaProjects/BigData/web-tag/src/main/resources/temptags.txt")
        val poi_taglist: RDD[(String, String)] = poi_tags.map(e => e.split("\t")).filter(e => e.length == 2)
                .map(e => e(0) -> ReviewTags.extractTags(e(1)))
                .filter(e => e._2.length > 0)
                .map(e => e._1 -> e._2.split(","))
                .flatMapValues(e => e)
                .map(e => (e._1, e._2) -> 1)
                .reduceByKey(_ + _)
                .map(e => e._1._1 -> List((e._1._2, e._2)))
                .reduceByKey(_ ::: _)
                .map(e => e._1 -> e._2.sortBy(_._2).reverse.take(20).map(a => a._1 + ":" + a._2.toString).mkString(","))
//        poi_taglist.map(e => e._1 + "\t" + e._2).saveAsTextFile("/Users/xiaows/IdeaProjects/BigData/web-tag/src/main/resources/res")
        val res: Array[(String, String)] = poi_taglist.collect()
        println(res.toBuffer)
    }
}
