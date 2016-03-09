package com.hason.study.rabbitmq.demo.chapter5;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * 第六章：使用主题类型Topic的交换器进行消息分发
 * 
 * 主题类型Topic的 Routing Key 不能任意命名，需要符合格式：xx.xx.xx（以英文句号分隔）
 * 有两个特殊符号：
 *   星号（*） 代表任意一个单词
 *   hash（#） 代表 0 个或者多个单词
 *   
 *   "#.*" 可以匹配 ： ".."、"word"、"a.b.c"、".a.b.c"、"a.b."
 * 
 * @author hason
 *
 */
public class Send {
	
	private static final String EXCHANGE_NAME= "exchange_ch5";
	
	public static void main(String[] args) throws IOException, TimeoutException {
		send("Test topic ");
	}

	public static void send(String msg) throws IOException, TimeoutException {
		// 1、建立连接，参数是RabbitMQ Server 的 ip 或者 name
		ConnectionFactory factory = new ConnectionFactory();
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		
		// 2、定义广播类型的交换器
		channel.exchangeDeclare(EXCHANGE_NAME, "topic");
		
		// 3、发送到交换器
		String routingKey = "1.2.";
		channel.basicPublish(EXCHANGE_NAME, routingKey, null, msg.getBytes("UTF-8"));
		
		channel.close();
		connection.close();
	}
}
