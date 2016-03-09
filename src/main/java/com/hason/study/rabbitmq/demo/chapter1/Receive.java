package com.hason.study.rabbitmq.demo.chapter1;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Date;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP.BasicProperties;

/**
 * 第二章：演示简单的队列消息Hello World流程
 * 
 * Receive 消息的接收者
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
public class Receive {
	
	private static final String QUEUE_NAME = "queue_hello";

	public static void main(String[] args) throws IOException, TimeoutException {
		receiveHelloWorld();
	}
	

	/**
	 * 2、接收队列消息
	 * @throws TimeoutException 
	 * @throws IOException 
	 */
	public static void receiveHelloWorld() throws IOException, TimeoutException {
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
		
		// 3、接收数据前，创建一个回调函数用于处理接收到的消息
		// handleDelivery 继承于 Consumer 接口
		// DefaultConsumer.handleDelivery() 是没有任何处理操作的，需要我们自己实现处理方法
		Consumer consumer = new DefaultConsumer(channel) {
			
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope,
					BasicProperties properties, byte[] body) throws IOException {
				String msg = new String(body, "UTF-8");
				System.out.println(MessageFormat.format("{0} - Received : {1}", new Date(), msg));
			}
			
		};
		
		// 4、接收消息
		channel.basicConsume(QUEUE_NAME, consumer);
		
		
	}
	
}
