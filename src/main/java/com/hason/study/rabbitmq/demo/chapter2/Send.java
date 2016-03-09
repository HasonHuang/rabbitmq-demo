package com.hason.study.rabbitmq.demo.chapter2;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Date;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * 第三章：任务分发机制
 * 
 * 1、循环分发（Round-robin）：默认值。状态下， RabbitMQ 将第 n 个Message 分发给第 n 个 Consumer。当然 n 是取余后的。
 *    它不管 Consumer 是否还有 unAcked Message，仍按照这个默认机制进行分发。
 *    缺点：若某个消费者A任务重，导致其他消费者B、C基本没事可做。
 *    
 * 2、公平分发（Fair dispatch）：通过 channel.basicQos() 方法设置 prefetch_count=1 。
 *    这样 RabbitMQ 就会使得每个 Consumer 在同一个时间点最多处理一个 Message。
 *    换句话说，在接收到该 Consumer 的 ack 前，他它不会将新的 Message 分发给它。 
 * 
 * @author hason
 *
 */
public class Send {
	
	private static final String QUEUE_NAME = "queue_hello";

	public static void main(String[] args) throws IOException, TimeoutException {
		String msg = getMessage(args);
		send(msg);
	}
	
	public static void send(String msg) throws IOException, TimeoutException {
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
		channel.basicPublish("", QUEUE_NAME, null, msg.getBytes());
		System.out.println(MessageFormat.format("{0} - Send : {1}", new Date(), msg));
		
		// 4、关闭连接
		channel.close();
		connection.close();
		
	}

	/**
	 * 用空格连接数组
	 * @param strings
	 * @return
	 */
	private static String getMessage(String[] strings) {
		return strings.length < 1 ? "Hello World!" : StringUtils.join(strings, " ");
	}

}
