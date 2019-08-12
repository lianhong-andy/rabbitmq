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

    public static void receiveWithReturn() {
        ConnectionFactory connectionFactory = MQUtils.getConnectionFactory("192.168.56.108", "/", 5672);

        String queueName = "test_return_queue";
        String exchangeName = "test_return_exchange";
        String exchangeType = "topic";
        String routingKey = "return.#";

        boolean durable = true;
        boolean exclusive = false;
        boolean autoDelete = false;
        Map<String,Object> otherArguments = null;
        boolean autoAck = true;//是否自动应答（监听）

        MQUtils.receiveWithReturn(connectionFactory,queueName,durable,exclusive,autoDelete,otherArguments,autoAck,routingKey,exchangeName,exchangeType);
    }

    public static void receiveWithMyConsumer() {
        ConnectionFactory connectionFactory = MQUtils.getConnectionFactory("192.168.56.108", "/", 5672);

        String queueName = "test_return_queue";
        String exchangeName = "test_return_exchange";
        String exchangeType = "topic";
        String routingKey = "return.#";

        boolean durable = true;
        boolean exclusive = false;
        boolean autoDelete = false;
        Map<String,Object> otherArguments = null;
        boolean autoAck = true;//是否自动应答（监听）

        MQUtils.receiveWithMyConsumer(connectionFactory,queueName,durable,exclusive,autoDelete,otherArguments,autoAck,routingKey,exchangeName,exchangeType);
    }

    public static void receiveWithMyQOS() {
        ConnectionFactory connectionFactory = MQUtils.getConnectionFactory("192.168.56.108", "/", 5672);

        String queueName = "test_qos_queue";
        String exchangeName = "test_qos_exchange";
        String exchangeType = "topic";
        String routingKey = "qos.#";

        boolean durable = true;
        boolean exclusive = false;
        boolean autoDelete = false;
        Map<String,Object> otherArguments = null;
        /**消费限流不能自动应答，必须是手动应答*/
        boolean autoAck = false;

        MQUtils.receiveWithQOS(connectionFactory,queueName,durable,exclusive,autoDelete,otherArguments,autoAck,routingKey,exchangeName,exchangeType);
    }

    public static void receiveWithReSend() {
        ConnectionFactory connectionFactory = MQUtils.getConnectionFactory("192.168.56.108", "/", 5672);

        String queueName = "test_ack_queue";
        String exchangeName = "test_ack_exchange";
        String exchangeType = "topic";
        String routingKey = "ack.#";

        boolean durable = true;
        boolean exclusive = false;
        boolean autoDelete = false;
        Map<String,Object> otherArguments = null;
        /**消费限流不能自动应答，必须是手动应答*/
        boolean autoAck = false;

        MQUtils.receiveWithReSend(connectionFactory,queueName,durable,exclusive,autoDelete,otherArguments,autoAck,routingKey,exchangeName,exchangeType);

    }
    //test reset command
}
