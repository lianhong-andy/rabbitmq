package com.andy.rabbitmq.consumer;

import com.andy.rabbitmq.common.MQUtils;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * @author lianhong
 * @description 消费者
 * @date 2019/7/31 0031下午 1:32
 */
public class Consumer {

    public static void receiveDirectQueue() {
        ConnectionFactory connectionFactory = MQUtils.getConnectionFactory("192.168.56.108","/",5672);

//        String queueName = "test001";
        String queueName = "test_direct_queue";
        boolean durable = true;
        boolean exclusive = false;
        boolean autoDelete = false;
        Map<String,Object> otherArguments = null;
        boolean autoAck = true;//是否自动应答（监听）

        MQUtils.receiveMsg(connectionFactory,queueName,durable,exclusive,autoDelete,otherArguments,autoAck);
    }


    public static void receiveTopic() {
        ConnectionFactory connectionFactory = MQUtils.getConnectionFactory("192.168.56.108","/",5672);

        String queueName = "test_topic_queue";
        String exchangeName = "test_topic_exchange";
        String exchangeType = "topic";
        String routingKey = "user.*";
        boolean durable = true;
        boolean exclusive = false;
        boolean autoDelete = false;
        Map<String,Object> otherArguments = null;
        boolean autoAck = true;//是否自动应答（监听）

        MQUtils.receiveTopic(connectionFactory,queueName,durable,exclusive,autoDelete,otherArguments,autoAck,routingKey,exchangeName,exchangeType);
    }

    public static void receiveFanout() {
        ConnectionFactory connectionFactory = MQUtils.getConnectionFactory("192.168.56.108","/",5672);

        String queueName = "test_fanout_queue";
        String exchangeName = "test_fanout_exchange";
        String exchangeType = "fanout";
        String routingKey = "";

        boolean durable = true;
        boolean exclusive = false;
        boolean autoDelete = false;
        Map<String,Object> otherArguments = null;
        boolean autoAck = true;//是否自动应答（监听）

        MQUtils.receiveFanout(connectionFactory,queueName,durable,exclusive,autoDelete,otherArguments,autoAck,routingKey,exchangeName,exchangeType);

    }

    public static void receiveWithProps() {
        ConnectionFactory connectionFactory = MQUtils.getConnectionFactory("192.168.56.108","/",5672);

        String queueName = "test_topic_queue";
        String exchangeName = "test_topic_exchange";
        String exchangeType = "topic";
        String routingKey = "user.#";

        boolean durable = true;
        boolean exclusive = false;
        boolean autoDelete = false;
        Map<String,Object> otherArguments = null;
        boolean autoAck = true;//是否自动应答（监听）

        MQUtils.receiveWithProps(connectionFactory,queueName,durable,exclusive,autoDelete,otherArguments,autoAck,routingKey,exchangeName,exchangeType);
    }

    public static void receiveWithConfirm() throws InterruptedException, TimeoutException, IOException {
        ConnectionFactory connectionFactory = MQUtils.getConnectionFactory("192.168.56.108", "/", 5672);

        String queueName = "test_confirm_queue";
        String exchangeName = "test_confirm_exchange";
        String exchangeType = "topic";
        String routingKey = "confirm.#";

        boolean durable = true;
        boolean exclusive = false;
        boolean autoDelete = false;
        Map<String,Object> otherArguments = null;
        boolean autoAck = true;//是否自动应答（监听）

        MQUtils.receiveWithConfirm(connectionFactory,queueName,durable,exclusive,autoDelete,otherArguments,autoAck,routingKey,exchangeName,exchangeType);

    }
}
