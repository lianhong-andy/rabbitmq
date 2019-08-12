package com.andy.rabbitmq.producer;

import com.andy.rabbitmq.common.MQUtils;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;

import java.util.HashMap;

/**
 * @author lianhong
 * @description 消息生产者
 * @date 2019/7/31 0031下午 1:31
 */
public class Producer {

    public static void send() {
        ConnectionFactory connectionFactory = MQUtils.getConnectionFactory("192.168.56.108","/",5672);


        //交换机
        /**
         * routingKey必须有，如果为空，则是默认exchange，此时会去寻找和routingKey同名的队列名称，
         * 如果找到，则可以路由过去,否则路由不过去
         */
        String exchange = "";
        //routingKey
//        String routingKey = "test001";
        String routingKey = "test_direct_queue";
        //附件条件
        AMQP.BasicProperties props = null;

        //信息
        String msg = "hello rabbitMQ!";

        //发消息
        MQUtils.sendMessage(connectionFactory,exchange,routingKey,props,msg);


    }


    public static void sendTopic(String exchange, String routingKey) {
        ConnectionFactory connectionFactory = MQUtils.getConnectionFactory("192.168.56.108","/",5672);

        //附件条件
        AMQP.BasicProperties props = null;

        //信息
        String msg = "hello rabbitMQ!";

        //发消息
        MQUtils.sendMessage(connectionFactory,exchange,routingKey,props,msg);


    }

    public static void sendFanout(String exchange, String routingKey) {
        ConnectionFactory connectionFactory = MQUtils.getConnectionFactory("192.168.56.108","/",5672);

        HashMap<String, Object> headers = new HashMap<>();
        headers.put("my1","111");
        headers.put("my2","222");
        //附件条件
        AMQP.BasicProperties props = new AMQP.BasicProperties().builder()
                .deliveryMode(2)//持久化投递，服务重启消息不丢失
                .contentEncoding("UTF-8")
                .expiration("30000")
                .headers(headers)//存储自定义属性
                .build();

        //信息
        String msg = "hello rabbitMQ!";

        //发消息
        MQUtils.sendMessage(connectionFactory,exchange,routingKey,props,msg);
    }

    public static void sendWithProps(String exchange, String routingKey,  AMQP.BasicProperties props) {
        ConnectionFactory connectionFactory = MQUtils.getConnectionFactory("192.168.56.108","/",5672);

        //信息
        String msg = "hello rabbitMQ!";

        //发消息
        MQUtils.sendMessage(connectionFactory,exchange,routingKey,props,msg);
    }


    public static void sendWithConfirm(String exchange, String routingKey,  AMQP.BasicProperties props) {
        ConnectionFactory connectionFactory = MQUtils.getConnectionFactory("192.168.56.108","/",5672);

        //信息
        String msg = "hello rabbitMQ!";

        //发消息
        MQUtils.sendMessageWithConfirm(connectionFactory,exchange,routingKey,props,msg);
    }

    public static void sendWithReturn(String exchange, String routingKey, AMQP.BasicProperties props) {
        ConnectionFactory connectionFactory = MQUtils.getConnectionFactory("192.168.56.108","/",5672);

        //信息
        String msg = "hello rabbitMQ!";

        //发消息
        MQUtils.sendMessageWithReturn(connectionFactory,exchange,routingKey,props,msg);
    }

    public static void sendWithQOS(String exchange, String routingKey, AMQP.BasicProperties props) {
        ConnectionFactory connectionFactory = MQUtils.getConnectionFactory("192.168.56.108","/",5672);


        //信息
        String msg = "hello rabbitMQ!";

        //发消息
        MQUtils.sendMessageQOS(connectionFactory,exchange,routingKey,props,msg);
    }

    public static void sendWithReSend(String exchange, String routingKey,  AMQP.BasicProperties props) {
        ConnectionFactory connectionFactory = MQUtils.getConnectionFactory("192.168.56.108","/",5672);

        //信息
        String msg = "hello rabbitMQ!";

        //发消息
        MQUtils.sendMessageWithReSend(connectionFactory,exchange,routingKey,props,msg);
    }
}
