package com.gaode

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils


object Http_gaode {

  def get(url:String):String={
    // 创建 client 实例
    val httpClient = HttpClients.createDefault()
    // 创建 get 实例
    val get = new HttpGet(url)

    val response = httpClient.execute(get)    // 发送请求
    EntityUtils.toString(response.getEntity,"UTF-8")    // 获取返回结果

  }
}

