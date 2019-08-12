package com.andy.rabbitmq;

import com.andy.rabbitmq.consumer.Consumer;
import com.andy.rabbitmq.producer.Producer;
import com.rabbitmq.client.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;

@RunWith(SpringRunner.class)
@SpringBootTest
public class RabbitmqApplicationTests {

    @Test
    public void contextLoads() throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setPort(5672);
        connectionFactory.setHost("192.168.56.108");
        connectionFactory.setVirtualHost("/");
//        connectionFactory.setUsername("admin");
//        connectionFactory.setPassword("admin");

        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        String queueName = "test001";
        channel.queueDeclare(queueName,true,false,false,null);

        QueueingConsumer queueingConsumer = new QueueingConsumer(channel);


        channel.basicConsume(queueName,true,queueingConsumer);

        while (true) {
            QueueingConsumer.Delivery delivery = queueingConsumer.nextDelivery();
            String msg = new String(delivery.getBody());

            System.out.println("msg = " + msg);



        }

    }

    @Test
    public void receive(){
        Consumer.receiveDirectQueue();
    }

    @Test
    public void receiveTopic(){
        Consumer.receiveTopic();
    }

    @Test
    public void receiveFanout(){
        Consumer.receiveFanout();
    }

    @Test
    public void receiveWithProps(){
        Consumer.receiveWithProps();
    }

    @Test
    public void receiveWithConfirm() throws InterruptedException, IOException, TimeoutException {
        Consumer.receiveWithConfirm();
    }

    @Test
    public void receiveWithReturn() throws InterruptedException, IOException, TimeoutException {
        Consumer.receiveWithReturn();
    }

    /**
     * 自定义监听
     */
    @Test
    public void receiveWithMyConsumer(){
        Consumer.receiveWithMyConsumer();
    }

    /**
     * 重回队列监听
     */
    @Test
    public void receiveWithReSend() throws InterruptedException, IOException, TimeoutException {
        Consumer.receiveWithReSend();
    }


    /**
     * 消费限流-消费端
     */
    @Test
    public void receiveWithQOS(){
        Consumer.receiveWithMyQOS();
    }

    @Test
    public void send(){
        Producer.send();
    }

    @Test
    public void sendTopic(){
        Producer.sendTopic("test_topic_exchange","user.");
    }

    @Test
    public void sendFanout(){
        Producer.sendFanout("test_fanout_exchange", "");
    }

    /**
     * 携带自定义属性
     */
    @Test
    public void sendWithProps(){
        HashMap<String, Object> headers = new HashMap<>();
        headers.put("my1","111");
        headers.put("my2","222");
        AMQP.BasicProperties props = new AMQP.BasicProperties().builder()
                .deliveryMode(2)//持久化投递，服务重启消息不丢失
                .contentEncoding("UTF-8")
                .expiration("30000")
                .headers(headers)//存储自定义属性
                .build();
        Producer.sendWithProps("test_topic_exchange","user.",props);
    }

    /**
     * 监听confirm
     */
    @Test
    public void sendWithConfirm(){
        Producer.sendWithConfirm("test_confirm_exchange","confirm.save",null);
    }


    /**
     * 监听return
     */
    @Test
    public void sendWithReturn(){
//        Producer.sendWithReturn("test_return_exchange","return.save",null);
        Producer.sendWithReturn("test_return_exchange","abc.save",null);
    }

    /**
     * 消费限流发送端
     */
    @Test
    public void sendWithQOS(){
        Producer.sendWithQOS("test_qos_exchange","qos.save",null);
    }

    /**
     * 重回队列-发送端
     */
    @Test
    public void sendWithReSend(){
        Producer.sendWithReSend("test_ack_exchange","ack.save",null);
    }




}
