package main.scala1.com.redis_idea

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * redis 连接
  */
object JedisConectionPool {

  val config = new JedisPoolConfig()

  //设置最大连接数
  config.setMaxTotal(20)
  //最大空闲
  config.setMaxIdle(10)
  //创建链接
  val pool = new JedisPool(config,"192.168.149.101",6379)

  def getConection() :Jedis ={
    pool.getResource
  }
}
