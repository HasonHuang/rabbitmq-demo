package com.hason.study.rabbitmq.demo.chapter3;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * 第四章：Publish/Subscribe模式（发布/订阅 模式）
 * 
 * 1、Send定义一个fanout类型的exchange（广播类型的交换器）
 * 2、Send发布消息到exchange
 * 3、Receive定义一个（步骤1中的）交换器，
 * 4、Receive声明一个临时队列（不指定名字时，RabbitMQ会随机为我们选择这个名字）
 * 5、Receive把 交换器 和 队列 进行绑定Bindings。
 * 5、Receive接收消息
 * 
 * @author hason
 *
 */
public class Send {
	
	private static final String EXCHANGE_NAME = "exchange_ch3";

	public static void main(String[] args) throws IOException, TimeoutException {
		sendByExchange("test public/subscribe to everyone");
	}
	
	public static void sendByExchange(String msg) throws IOException, TimeoutException {
		// 1、建立连接，参数是RabbitMQ Server 的 ip 或者 name
		ConnectionFactory factory = new ConnectionFactory();
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		
		// 2、定义广播类型的交换器
		channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
		
		// 3、发送到交换器
		channel.basicPublish(EXCHANGE_NAME, "", null, msg.getBytes("UTF-8"));
		
		// 4、关闭连接
		channel.close();
		connection.close();
	}
}
