package tools

import java.io.{File, FileInputStream}
import java.util.Properties

import redis.clients.jedis.{Jedis,JedisPool, JedisPoolConfig}


object RedisClientUtils extends Serializable {
  @transient var jedispool:JedisPool=null

  def initJedisPool:Unit={
    val properties = new Properties()
    val file = new File("config/config.properties")
    if(file.exists()){
      val fileInput = new FileInputStream(file)
      properties.load(fileInput)
      val jedisPoolConfig = new JedisPoolConfig
      jedisPoolConfig.setMaxIdle((properties.getProperty("redis.pool.maxIdle")).toInt)
      jedisPoolConfig.setMaxTotal((properties.getProperty("redis.pool.maxActive")).toInt)
      jedisPoolConfig.setMinIdle((properties.getProperty("redis.pool.minIdle")).toInt)
      jedisPoolConfig.setMaxWaitMillis(properties.getProperty("redis.pool.maxWait").toInt)
      jedisPoolConfig.setTestOnBorrow(true)
      jedisPoolConfig.setTestOnCreate(true)
      jedisPoolConfig.setTestOnReturn(true)
      val host = properties.getProperty("redis.host")
      val port = properties.getProperty("redis.port").toInt
      val timeOut = properties.getProperty("redis.timeout").toInt
      jedispool = new JedisPool(jedisPoolConfig,host,port,timeOut)
    }
  }

  def getJedis:Jedis={
    if(jedispool==null){
      initJedisPool
    }
    jedispool.getResource
  }
}
