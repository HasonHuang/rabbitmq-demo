package com.hason.study.rabbitmq.demo.chapter2;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP.BasicProperties;

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
public class Receive {

	private static final String QUEUE_NAME = "queue_hello";
	
	public static void main(String[] args) throws IOException, TimeoutException {
		reveive();
		shutdown();
	}
	
	public static void reveive() throws IOException, TimeoutException {
		// 1、建立连接，参数是RabbitMQ Server 的 ip 或者 name
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		final Channel channel = connection.createChannel();
		
		// 2、创建一个队列
		// durable : 重启后是否恢复内容；如果true，消息会持久化到磁盘
		// exclusive : 仅创建者可以使用的私有队列，断开后自动删除
		// autoDelete : 当所有消费客户端连接断开后，是否自动删除队列
		channel.queueDeclare(QUEUE_NAME, false, false, false, null);
		
		// 均匀分配消息给每个worker，表示RabbitMQ同一时间发给消费者的消息不超过一条
		channel.basicQos(1);
		
		// 3、接收数据前，创建一个回调函数用于处理接收到的消息
		// handleDelivery 继承于 Consumer 接口
		// DefaultConsumer.handleDelivery() 是没有任何处理操作的，需要我们自己实现处理方法
		Consumer callback = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope,
					BasicProperties properties, byte[] body) throws IOException {
				String msg = new String(body, "UTF-8");
				System.out.println(MessageFormat.format("{0} - Receive: {1}", new Date(), msg));
				
				try {
					doWork(msg);
				} catch (InterruptedException e) {
					e.printStackTrace();
				} finally {
					System.out.println(MessageFormat.format("{0} - Receive: Done", new Date()));
					// 发送ack确认消息，与channel.basicConsume()对应
					channel.basicAck(envelope.getDeliveryTag(), false);
				}
			}
		};
		
		// 4、接收消息
		channel.basicConsume(QUEUE_NAME, callback);
	}
	
	/**
	 * 模拟实际项目的复杂计算。
	 * 逐个字符地检查参数msg，每个小数点"."，就会睡眠已秒
	 * @param msg
	 * @throws InterruptedException
	 */
	private static void doWork(String msg) throws InterruptedException {
		for(char c : msg.toCharArray()) {
			if(c == '.')
				TimeUnit.SECONDS.sleep(1);
		}
	}
	
	/**
	 * 阻塞程序，直到输入命令关闭
	 */
	private static void shutdown() throws IOException {
		System.out.println(MessageFormat.format("{0} - press enter to exit.", new Date()));
		while(true) {
			char c = (char) System.in.read();
			if(c == '\n')
				System.exit(-1);
		}
	}
	
}
