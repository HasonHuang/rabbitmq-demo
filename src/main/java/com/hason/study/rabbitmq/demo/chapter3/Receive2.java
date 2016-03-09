package com.hason.study.rabbitmq.demo.chapter3;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 第四章：Publish/Subscribe模式（发布/订阅 模式）
 * 
 * Receive2 模拟多个订阅者之一
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
public class Receive2 {

	public static void main(String[] args) throws IOException, TimeoutException {
		Receive1.receive();
	}
}
