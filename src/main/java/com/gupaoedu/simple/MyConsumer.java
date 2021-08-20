package com.gupaoedu.simple;

import com.rabbitmq.client.*;

import java.io.IOException;

/**
 * @author Mike
 * @date 2021/8/20 0020 14:34
 *消费者
 */
public class MyConsumer {
    private final static String EXCHANGE_NAME = "SIMPLE_EXCHANGE";
    private final static String QUEUE_NAME = "SIMPLE_QUEUE";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        // 连接IP
        factory.setHost("127.0.0.1");
        // 默认监听端口
        factory.setPort(5672);
        // 虚拟机
        factory.setVirtualHost("/");

        // 设置访问的用户
        factory.setUsername("guest");
        factory.setPassword("guest");
        // 建立连接
        Connection conn = factory.newConnection();
        // 创建消息通道  TCP连接 在连接上创建一个channel
        Channel channel = conn.createChannel();

        //下面比较重要的部分
        // 声明交换机
        // String exchange, String type, boolean durable, boolean autoDelete, Map<String, Object> arguments
        channel.exchangeDeclare(EXCHANGE_NAME,"direct",false, false, null);

//        声明交换机的参数：
//　　String type：交换机的类型，direct, topic, fanout中的一种。
//　　boolean durable：是否持久化，代表交换机在服务器重启后是否还存在。
//　　boolean autoDelete：是否自动删除。

        // 声明队列
        // String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(" Waiting for message....");

//        声明队列的参数：
//        　　boolean durable：是否持久化，代表队列在服务器重启后是否还存在。
//　　boolean exclusive：是否排他性队列。排他性队列只能在声明它的Connection中使用，连接断开时自动删除。
//　　boolean autoDelete：是否自动删除。如果为true，至少有一个消费者连接到这个队列，之后所有与这个队列连接的消费者都断开时，队列会自动删除。
//　　Map<String, Object> arguments：队列的其他属性，例如x-message-ttl、x-expires、x-max-length、x-max-length-bytes、x-dead-letter-exchange、x-dead-letter-routing-key、x-max-priority。

        // 绑定队列和交换机  指定一个路由键 因为他是直连类型的交换机  路由键和绑定键完全对应
        channel.queueBind(QUEUE_NAME,EXCHANGE_NAME,"gupao.best");

        // 创建消费者 指定一个consumer 哪一个队列中获取消息
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                String msg = new String(body, "UTF-8");
                System.out.println("Received message : '" + msg + "'");
                System.out.println("consumerTag : " + consumerTag );
                System.out.println("deliveryTag : " + envelope.getDeliveryTag() );
            }
        };

        //        消息属性BasicProperties：
//　　消息的全部属性有14个，以下列举了一些主要的参数：
//　　Map<String,Object> headers 消息的其他自定义参数
//　　Integer deliveryMode 2持久化，其他：瞬态
//　　Integer priority 消息的优先级
//　　String correlationId 关联ID，方便RPC相应与请求关联
//　　String replyTo 回调队列
//　　String expiration TTL，消息过期时间，单位毫秒

        // 开始获取消息
        // String queue, boolean autoAck, Consumer callback
        channel.basicConsume(QUEUE_NAME, true, consumer);
    }
}
