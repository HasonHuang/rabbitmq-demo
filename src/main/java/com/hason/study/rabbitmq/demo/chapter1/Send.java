package com.hason.study.rabbitmq.demo.chapter1;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Date;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * 第二章：演示简单的队列消息Hello World流程
 * 
 * Send 消息的发送者
 * 
 * 1、通过 ConnectionFactory 创建连接 Connection
 * 2、通过 Connection 创建 Channel（数据流动都是在 Channel 中进行）
 * 3、声明一个队列
 * 4、发送消息
 * 5、关闭连接
 * 
 * @author hason
 *
 */
public class Send {

	private static final String QUEUE_NAME = "queue_hello";
	
	public static void main(String[] args) throws IOException, TimeoutException {
		sendHelloWorld();
		
	}
	
	/**
	 * 1、发送 Hello World 到队列
	 * @throws TimeoutException 
	 * @throws IOException 
	 */
	public static void sendHelloWorld() throws IOException, TimeoutException {
		// 1、建立连接，参数是RabbitMQ Server 的 ip 或者 name
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		
		// 2、创建一个队列
		// durable : 重启后是否恢复内容；如果true，消息会持久化到磁盘
		// exclusive : 仅创建者可以使用的私有队列，断开后自动删除
		// autoDelete : 当所有消费客户端连接断开后，是否自动删除队列
		channel.queueDeclare(QUEUE_NAME, false, false, false, null);
		
		// 3、发送消息
		String msg = "Hello World";
		channel.basicPublish("", QUEUE_NAME, null, msg.getBytes());
		System.out.println(MessageFormat.format("{0} - Send : {1}", new Date(), msg));
		
		// 4、关闭连接
		channel.close();
		connection.close();
		
	}
	
}
