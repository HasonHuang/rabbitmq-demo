package com.hason.study.rabbitmq.demo.chapter3;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Date;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * 第四章：Publish/Subscribe模式（发布/订阅 模式）
 * 
 * Receive1 模拟多个订阅者之一
 * 
 * 1、            Send定义一个fanout类型的exchange（广播类型的交换器）
 * 2、            Send发布消息到exchange
 * 3、Receive定义一个（步骤1中的）交换器，
 * 4、Receive声明一个临时队列（不指定名字时，RabbitMQ会随机为我们选择这个名字）
 * 5、Receive把 交换器 和 队列 进行绑定Bindings。
 * 5、Receive接收消息
 * 
 * @author hason
 *
 */
public class Receive1 {

	private static final String EXCHANGE_NAME = "exchange_ch3";
	
	public static void main(String[] args) throws IOException, TimeoutException {
		receive();
	}
	
	public static void receive() throws IOException, TimeoutException {
		// 1、建立连接，参数是RabbitMQ Server 的 ip 或者 name
		ConnectionFactory factory = new ConnectionFactory();
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		
		// 2、定义广播类型的交换器
		channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
		// 3、声明一个队列，由RabbitMQ随机生成名字
		String queueName = channel.queueDeclare().getQueue();  // RabbitMQ随机生成的队列名
		// 4、把交换器和队列进行绑定
		channel.queueBind(queueName, EXCHANGE_NAME, "");
		
		// 5、接收并处理消息
		Consumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope,
					BasicProperties properties, byte[] body) throws IOException {
				String msg = new String(body, "UTF-8");
				System.out.println(MessageFormat.format("{0} - get message: {1}", new Date(), msg));
			}
		};
		channel.basicConsume(queueName, true, consumer); // 自动反馈确认信息
		
		// 6、关闭连接。不关闭代表一直监听
//		channel.close();
//		connection.close();
		
	}
}
