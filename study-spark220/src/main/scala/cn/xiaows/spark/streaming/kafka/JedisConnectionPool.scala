package cn.xiaows.spark.streaming.kafka

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisConnectionPool {

	private val config = new JedisPoolConfig()
	config.setMaxTotal(20)
	config.setMaxIdle(10)
	config.setTestOnBorrow(true)

	private val pool = new JedisPool(config, "localhost", 6379, 10000)

	def getConnection() ={
		pool.getResource
	}

	def main(args: Array[String]): Unit = {
		val conn: Jedis = JedisConnectionPool.getConnection()
		import scala.collection.JavaConversions._
		for (k <- conn.keys("*")) {
			println(k, conn.get(k))
		}
	}
}
