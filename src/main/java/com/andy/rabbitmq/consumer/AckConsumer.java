package com.andy.rabbitmq.consumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;

/**
 * @author lianhong
 * @description
 * @date 2019/8/10 0010下午 6:38
 */
public class AckConsumer extends DefaultConsumer {
    Channel channel;

    public AckConsumer(Channel channel) {
        super(channel);
        this.channel = channel;
    }


    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        System.out.println("====================consumer message===============");
        System.out.println("consumerTag = " + consumerTag);
        System.out.println("envelope = " + envelope.toString());
        System.out.println("properties = " + properties.toString());
        System.out.println("body = " + new String(body));

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(properties.getHeaders().get("num"));
        if ((Integer)properties.getHeaders().get("num") == 0) {
            /**
             *  param1 - deliveryTag
             *  param2 - multiple
             *  param3 - requeue 是否重回队列 true-重回队列
             */
            channel.basicNack(envelope.getDeliveryTag(),false,true);
        } else {
            /**
             *  param1 - deliveryTag
             *  param2 - multiple
             */
            channel.basicAck(envelope.getDeliveryTag(),false);
        }

    }
}
