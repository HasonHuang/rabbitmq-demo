package com.hason.study.rabbitmq.demo.chapter5;

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
public class Receive {

	private static final String EXCHANGE_NAME = "exchange_ch5";
	
	public static void main(String[] args) throws IOException, TimeoutException {
		receive();
	}
	
	public static void receive() throws IOException, TimeoutException {
		// 1、建立连接，参数是RabbitMQ Server 的 ip 或者 name
		ConnectionFactory factory = new ConnectionFactory();
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		
		// 2、定义广播类型的交换器
		channel.exchangeDeclare(EXCHANGE_NAME, "topic");
		// 3、声明一个队列，由RabbitMQ随机生成名字
		String queueName = channel.queueDeclare().getQueue();  // RabbitMQ随机生成的队列名
		// 4、把交换器和队列进行绑定
		// 为了不和basicPublish中的 routingKey 混淆，这里把它称作绑定键 Binding key
		String bindingKey = "#.*";
		channel.queueBind(queueName, EXCHANGE_NAME, bindingKey);
		
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
