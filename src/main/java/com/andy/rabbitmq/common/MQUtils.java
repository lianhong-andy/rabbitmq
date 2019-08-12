package com.andy.rabbitmq.common;

import com.andy.rabbitmq.consumer.MyConsumer;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * @author lianhong
 * @description
 * @date 2019/7/31 0031下午 10:16
 */
public class MQUtils {
    public static ConnectionFactory getConnectionFactory(String host, String vhost, int port) {
        //创建一个ConnectionFactory，并进行配置
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(host);
        connectionFactory.setVirtualHost(vhost);
        connectionFactory.setPort(port);
        connectionFactory.setConnectionTimeout(100);

        return connectionFactory;
    }

    /**
     * 发消息
     *
     * @param connectionFactory 连接工厂
     * @param exchange          交换机
     * @param routingKey        路由
     * @param props             附加条件
     * @param msg               消息内容
     */
    public static void sendMessage(ConnectionFactory connectionFactory, String exchange,
                                   String routingKey, AMQP.BasicProperties props, String msg) {
        Connection connection = null;
        Channel channel = null;
        try {
            //通过连接工厂创建连接
            connection = connectionFactory.newConnection();

            //通过connection创建一个channel
            channel = connection.createChannel();

            //通过channel发送数据
            for (int i = 0; i < 5; i++) {
                routingKey += i;
                if (i % 2 != 0) {
                    routingKey += "." + i;
                }
                channel.basicPublish(exchange, routingKey, props, msg.getBytes());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        } finally {
            closeConnection(connection, channel);
        }

    }

    /**
     * 发消息
     *
     * @param connectionFactory 连接工厂
     * @param exchange          交换机
     * @param routingKey        路由
     * @param props             附加条件
     * @param msg               消息内容
     */
    public static void sendMessageWithReturn(ConnectionFactory connectionFactory, String exchange,
                                             String routingKey, AMQP.BasicProperties props, String msg) {
        Connection connection = null;
        Channel channel = null;
        try {
            //通过连接工厂创建连接
            connection = connectionFactory.newConnection();

            //通过connection创建一个channel
            channel = connection.createChannel();


            channel.addReturnListener(new ReturnListener() {

                @Override
                public void handleReturn(int replyCode, String replyText, String exchange, String routingKey,
                                         AMQP.BasicProperties basicProperties, byte[] bytes) throws IOException {
                    System.out.println("=================handle return================");
                    System.out.println("replyCode:" + replyCode);
                    System.out.println("replyText:" + replyText);
                    System.out.println("exchange:" + exchange);
                    System.out.println("routingKey:" + routingKey);
                    System.out.println("basicProperties:" + basicProperties);
                    System.out.println("body:" + new String(bytes));

                }
            });

            //通过channel发送数据
            for (int i = 0; i < 5; i++) {
                routingKey += i;
                if (i % 2 != 0) {
                    routingKey += "." + i;
                }
                /**
                 * 如果为true，则监听器会接收到路由不可达的消息，然后进行后续的处理，
                 * 如果为false，那么broker端会自动删除该消息！
                 * */
//                boolean mandatory = true;
                /**
                 * =================handle return================
                 * replyCode:312
                 * replyText:NO_ROUTE
                 * exchange:test_return_exchange
                 * routingKey:abc.save01.123.34
                 * basicProperties:#contentHeader<basic>(content-type=null, content-encoding=null, headers=null, delivery-mode=null, priority=null, correlation-id=null, reply-to=null, expiration=null, message-id=null, timestamp=null, type=null, user-id=null, app-id=null, cluster-id=null)
                 * body:hello rabbitMQ!
                 */
                boolean mandatory = false;
                channel.basicPublish(exchange, routingKey, mandatory, props, msg.getBytes());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }


    }

    public static void sendMessageQOS(ConnectionFactory connectionFactory, String exchange, String routingKey, AMQP.BasicProperties props, String msg) {

        Connection connection = null;
        Channel channel = null;
        try {
            //通过连接工厂创建连接
            connection = connectionFactory.newConnection();

            //通过connection创建一个channel
            channel = connection.createChannel();

            for (int i = 0; i < 5; i++) {
                channel.basicPublish(exchange, routingKey, props, msg.getBytes());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }





    /**
     * 发消息
     *
     * @param connectionFactory 连接工厂
     * @param exchange          交换机
     * @param routingKey        路由
     * @param props             附加条件
     * @param msg               消息内容
     */
    public static void sendMessageWithConfirm(ConnectionFactory connectionFactory, String exchange,
                                              String routingKey, AMQP.BasicProperties props, String msg) {
        Connection connection = null;
        Channel channel = null;
        try {
            //通过连接工厂创建连接
            connection = connectionFactory.newConnection();

            //通过connection创建一个channel
            channel = connection.createChannel();

            channel.confirmSelect();

            channel.addConfirmListener(new ConfirmListener() {
                @Override
                public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                    System.out.println("==========ack===================");
                }

                @Override
                public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                    System.out.println("==================no ack=============");
                }
            });

            //通过channel发送数据
            for (int i = 0; i < 5; i++) {
                routingKey += i;
                if (i % 2 != 0) {
                    routingKey += "." + i;
                }
                channel.basicPublish(exchange, routingKey, props, msg.getBytes());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }


    }

    /**
     * @param connectionFactory
     * @param queueName         队列名称
     * @param durable           true代表持久化，其实服务器重启消息也不会丢失
     * @param exclusive         是否独占（保证顺序消费，当上一个消息被所有集群节点消费完之后才消费下一个消息）
     *                          这个队列只有一个channel可以监听，相当于加锁
     * @param autoDelete        是否自动删除，当队列脱离exchange的绑定关系时自动删除
     * @param otherArguments    其它参数
     * @param autoAck           true代表自动应答（监听）,当broker发送一条消息到指定队列的时候，
     *                          消费者监听到会立马回调broker告诉它消息已消费，不管消费结果如何，也可以在代码中指定手动应答
     */
    public static void receiveMsg(ConnectionFactory connectionFactory, String queueName, boolean durable,
                                  boolean exclusive, boolean autoDelete, Map<String, Object> otherArguments, boolean autoAck) {
        Connection connection = null;
        Channel channel = null;

        try {

            //通过连接工厂创建连接
            //是否支持自动重连（用于网路发生闪断后的重连）
            connectionFactory.setAutomaticRecoveryEnabled(true);
            //每3s重连一次
            connectionFactory.setNetworkRecoveryInterval(3000);
            connection = connectionFactory.newConnection();

            //通过connection创建一个channel
            channel = connection.createChannel();

            String exchangeName = "test_direct_exchange";
            String exchangeType = "direct";
            String routingKey = "test.direct";

            //声明一个交换机
            channel.exchangeDeclare(exchangeName, exchangeType, durable, autoDelete, false, null);

            //声明队列
            channel.queueDeclare(queueName, durable, exclusive, autoDelete, otherArguments);

            //声明一个绑定关系
            channel.queueBind(queueName, exchangeName, routingKey);

            //创建消费者
            QueueingConsumer queueingConsumer = new QueueingConsumer(channel);

            //设置channel
            channel.basicConsume(queueName, autoAck, queueingConsumer);

            //获取消息

            while (true) {
                //一直阻塞方法
                QueueingConsumer.Delivery delivery = queueingConsumer.nextDelivery();
                //可设置超时时间，消费者启动了之后阻塞10s，10s之后没有监听调消息就放行
                //queueingConsumer.nextDelivery(1000);

                String msg = new String(delivery.getBody());
                System.out.println("消费端 = " + msg);
                Envelope envelope = delivery.getEnvelope();

                long deliveryTag = envelope.getDeliveryTag();//消息唯一性处理/应答的时候用到deliveryTag
                System.out.println("envelope = " + envelope);
            }


        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            closeConnection(connection, channel);
        }
    }

    /**
     * @param connectionFactory
     * @param queueName         队列名称
     * @param durable           true代表持久化，其实服务器重启消息也不会丢失
     * @param exclusive         是否独占（保证顺序消费，当上一个消息被所有集群节点消费完之后才消费下一个消息）
     *                          这个队列只有一个channel可以监听，相当于加锁
     * @param autoDelete        是否自动删除，当队列脱离exchange的绑定关系时自动删除
     * @param otherArguments    其它参数
     * @param autoAck           true代表自动应答（监听）,当broker发送一条消息到指定队列的时候，
     *                          消费者监听到会立马回到broker告诉它消息已消费，不管消费结果如何，也可以在代码中指定手动应答
     */
    public static void receiveTopic(ConnectionFactory connectionFactory, String queueName, boolean durable,
                                    boolean exclusive, boolean autoDelete, Map<String, Object> otherArguments,
                                    boolean autoAck, String routingKey, String exchangeName, String exchangeType) {
        Connection connection = null;
        Channel channel = null;

        try {

            //通过连接工厂创建连接
            //是否支持自动重连（用于网路发生闪断后的重连）
            connectionFactory.setAutomaticRecoveryEnabled(true);
            //每3s重连一次
            connectionFactory.setNetworkRecoveryInterval(3000);
            connection = connectionFactory.newConnection();

            //通过connection创建一个channel
            channel = connection.createChannel();

            //声明一个交换机
            channel.exchangeDeclare(exchangeName, exchangeType, durable, autoDelete, false, null);

            //声明队列
            channel.queueDeclare(queueName, durable, exclusive, autoDelete, otherArguments);

            //声明一个绑定关系
            channel.queueBind(queueName, exchangeName, routingKey);

            //创建消费者
            QueueingConsumer queueingConsumer = new QueueingConsumer(channel);

            //设置channel
            channel.basicConsume(queueName, autoAck, queueingConsumer);

            //获取消息

            while (true) {
                //一直阻塞方法
                QueueingConsumer.Delivery delivery = queueingConsumer.nextDelivery();
                //可设置超时时间，消费者启动了之后阻塞10s，10s之后没有监听调消息就放行
                //queueingConsumer.nextDelivery(1000);

                String msg = new String(delivery.getBody());
                System.out.println("消费端 = " + msg);
                Envelope envelope = delivery.getEnvelope();

                long deliveryTag = envelope.getDeliveryTag();//消息唯一性处理/应答的时候用到deliveryTag
                System.out.println("envelope = " + envelope);
            }


        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            closeConnection(connection, channel);
        }
    }

    /**
     * 关闭资源
     *
     * @param connection
     * @param channel
     */
    private static void closeConnection(Connection connection, Channel channel) {
        try {
            if (channel != null) {
                channel.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void receiveFanout(ConnectionFactory connectionFactory, String queueName, boolean durable,
                                     boolean exclusive, boolean autoDelete, Map<String, Object> otherArguments,
                                     boolean autoAck, String routingKey, String exchangeName, String exchangeType) {
        Connection connection = null;
        Channel channel = null;

        try {

            //通过连接工厂创建连接
            //是否支持自动重连（用于网路发生闪断后的重连）
            connectionFactory.setAutomaticRecoveryEnabled(true);
            //每3s重连一次
            connectionFactory.setNetworkRecoveryInterval(3000);
            connection = connectionFactory.newConnection();

            //通过connection创建一个channel
            channel = connection.createChannel();

            //声明一个交换机
            channel.exchangeDeclare(exchangeName, exchangeType, durable, autoDelete, false, null);

            //声明队列
            channel.queueDeclare(queueName, durable, exclusive, autoDelete, otherArguments);

            //声明一个绑定关系
            channel.queueBind(queueName, exchangeName, routingKey);

            //创建消费者
            QueueingConsumer queueingConsumer = new QueueingConsumer(channel);

            //设置channel
            channel.basicConsume(queueName, autoAck, queueingConsumer);

            //获取消息

            while (true) {
                //一直阻塞方法
                QueueingConsumer.Delivery delivery = queueingConsumer.nextDelivery();
                //可设置超时时间，消费者启动了之后阻塞10s，10s之后没有监听调消息就放行
                //queueingConsumer.nextDelivery(1000);

                String msg = new String(delivery.getBody());
                System.out.println("消费端 = " + msg);
                Envelope envelope = delivery.getEnvelope();

                long deliveryTag = envelope.getDeliveryTag();//消息唯一性处理/应答的时候用到deliveryTag
                System.out.println("envelope = " + envelope);

                String expiration = delivery.getProperties().getExpiration();
                System.out.println("expiration = " + expiration);

                Object o = delivery.getProperties().getHeaders().get("my1");
                System.out.println("o = " + o);

                String contentEncoding = delivery.getProperties().getContentEncoding();
                System.out.println("contentEncoding = " + contentEncoding);
            }


        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            closeConnection(connection, channel);
        }
    }

    public static void receiveWithProps(ConnectionFactory connectionFactory, String queueName, boolean durable,
                                        boolean exclusive, boolean autoDelete, Map<String, Object> otherArguments,
                                        boolean autoAck, String routingKey, String exchangeName, String exchangeType) {
        Connection connection = null;
        Channel channel = null;

        try {

            //通过连接工厂创建连接
            //是否支持自动重连（用于网路发生闪断后的重连）
            connectionFactory.setAutomaticRecoveryEnabled(true);
            //每3s重连一次
            connectionFactory.setNetworkRecoveryInterval(3000);
            connection = connectionFactory.newConnection();

            //通过connection创建一个channel
            channel = connection.createChannel();

            //声明一个交换机
            channel.exchangeDeclare(exchangeName, exchangeType, durable, autoDelete, false, null);

            //声明队列
            channel.queueDeclare(queueName, durable, exclusive, autoDelete, otherArguments);

            //声明一个绑定关系
            channel.queueBind(queueName, exchangeName, routingKey);

            //创建消费者
            QueueingConsumer queueingConsumer = new QueueingConsumer(channel);

            //设置channel
            channel.basicConsume(queueName, autoAck, queueingConsumer);

            //获取消息

            while (true) {
                //一直阻塞方法
                QueueingConsumer.Delivery delivery = queueingConsumer.nextDelivery();
                //可设置超时时间，消费者启动了之后阻塞10s，10s之后没有监听调消息就放行
                //queueingConsumer.nextDelivery(1000);

                String msg = new String(delivery.getBody());
                System.out.println("消费端 = " + msg);
                Envelope envelope = delivery.getEnvelope();

                long deliveryTag = envelope.getDeliveryTag();//消息唯一性处理/应答的时候用到deliveryTag
                System.out.println("envelope = " + envelope);

                String expiration = delivery.getProperties().getExpiration();
                System.out.println("expiration = " + expiration);

                Object o = delivery.getProperties().getHeaders().get("my1");
                System.out.println("o = " + o);
            }


        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            closeConnection(connection, channel);
        }

    }

    public static void receiveWithConfirm(ConnectionFactory connectionFactory, String queueName, boolean durable,
                                          boolean exclusive, boolean autoDelete, Map<String, Object> otherArguments,
                                          boolean autoAck, String routingKey, String exchangeName, String exchangeType) throws IOException, TimeoutException, InterruptedException {

        Connection connection = null;
        Channel channel = null;


        //通过连接工厂创建连接
        //是否支持自动重连（用于网路发生闪断后的重连）
        connectionFactory.setAutomaticRecoveryEnabled(true);
        //每3s重连一次
        connectionFactory.setNetworkRecoveryInterval(3000);
        connection = connectionFactory.newConnection();

        //通过connection创建一个channel
        channel = connection.createChannel();

        //声明一个交换机
        channel.exchangeDeclare(exchangeName, exchangeType, durable);

        //声明队列
        channel.queueDeclare(queueName, durable, exclusive, autoDelete, otherArguments);

        //声明一个绑定关系
        channel.queueBind(queueName, exchangeName, routingKey);

        //创建消费者
        QueueingConsumer queueingConsumer = new QueueingConsumer(channel);

        //设置channel
        channel.basicConsume(queueName, autoAck, queueingConsumer);

        //获取消息

        while (true) {
            //一直阻塞方法
            QueueingConsumer.Delivery delivery = queueingConsumer.nextDelivery();

            String msg = new String(delivery.getBody());
            System.out.println("消费端 = " + msg);
            Envelope envelope = delivery.getEnvelope();

            long deliveryTag = envelope.getDeliveryTag();//消息唯一性处理/应答的时候用到deliveryTag
            System.out.println("envelope = " + envelope);

        }
    }

    public static void receiveWithReturn(ConnectionFactory connectionFactory, String queueName, boolean durable,
                                         boolean exclusive, boolean autoDelete, Map<String, Object> otherArguments,
                                         boolean autoAck, String routingKey, String exchangeName, String exchangeType) {
        Connection connection = null;
        Channel channel = null;

        try {

            //通过连接工厂创建连接
            //是否支持自动重连（用于网路发生闪断后的重连）
            connectionFactory.setAutomaticRecoveryEnabled(true);
            //每3s重连一次
            connectionFactory.setNetworkRecoveryInterval(3000);
            connection = connectionFactory.newConnection();

            //通过connection创建一个channel
            channel = connection.createChannel();

            //声明一个交换机
            channel.exchangeDeclare(exchangeName, exchangeType, durable, autoDelete, false, null);

            //声明队列
            channel.queueDeclare(queueName, durable, exclusive, autoDelete, otherArguments);

            //声明一个绑定关系
            channel.queueBind(queueName, exchangeName, routingKey);

            //创建消费者
            QueueingConsumer queueingConsumer = new QueueingConsumer(channel);

            //设置channel
            channel.basicConsume(queueName, autoAck, queueingConsumer);

            //获取消息

            while (true) {
                //一直阻塞方法
                QueueingConsumer.Delivery delivery = queueingConsumer.nextDelivery();
                //可设置超时时间，消费者启动了之后阻塞10s，10s之后没有监听调消息就放行
                //queueingConsumer.nextDelivery(1000);

                String msg = new String(delivery.getBody());
                System.out.println("消费端 = " + msg);
                Envelope envelope = delivery.getEnvelope();

                long deliveryTag = envelope.getDeliveryTag();//消息唯一性处理/应答的时候用到deliveryTag
                System.out.println("envelope = " + envelope);
            }


        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void receiveWithMyConsumer(ConnectionFactory connectionFactory, String queueName, boolean durable,
                                             boolean exclusive, boolean autoDelete, Map<String, Object> otherArguments,
                                             boolean autoAck, String routingKey, String exchangeName, String exchangeType) {
        Connection connection = null;
        Channel channel = null;

        try {

            //通过连接工厂创建连接
            //是否支持自动重连（用于网路发生闪断后的重连）
            connectionFactory.setAutomaticRecoveryEnabled(true);
            //每3s重连一次
            connectionFactory.setNetworkRecoveryInterval(3000);
            connection = connectionFactory.newConnection();

            //通过connection创建一个channel
            channel = connection.createChannel();

            //声明一个交换机
            channel.exchangeDeclare(exchangeName, exchangeType, durable, autoDelete, false, null);

            //声明队列
            channel.queueDeclare(queueName, durable, exclusive, autoDelete, otherArguments);

            //声明一个绑定关系
            channel.queueBind(queueName, exchangeName, routingKey);

            //创建消费者
            QueueingConsumer queueingConsumer = new QueueingConsumer(channel);

            //设置channel
            channel.basicConsume(queueName, autoAck, new MyConsumer(channel));

            while (true) {

            }


        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    public static void receiveWithQOS(ConnectionFactory connectionFactory, String queueName, boolean durable,
                                             boolean exclusive, boolean autoDelete, Map<String, Object> otherArguments,
                                             boolean autoAck, String routingKey, String exchangeName, String exchangeType) {
        Connection connection = null;
        Channel channel = null;

        try {

            //通过连接工厂创建连接
            //是否支持自动重连（用于网路发生闪断后的重连）
            connectionFactory.setAutomaticRecoveryEnabled(true);
            //每3s重连一次
            connectionFactory.setNetworkRecoveryInterval(3000);
            connection = connectionFactory.newConnection();

            //通过connection创建一个channel
            channel = connection.createChannel();

            //声明一个交换机
            channel.exchangeDeclare(exchangeName, exchangeType, durable, autoDelete, false, null);

            //声明队列
            channel.queueDeclare(queueName, durable, exclusive, autoDelete, otherArguments);

            //声明一个绑定关系
            channel.queueBind(queueName, exchangeName, routingKey);

            channel.basicQos(0,1,false);

            //设置channel
            channel.basicConsume(queueName, autoAck, new MyConsumer(channel));

            while (true) {

            }


        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }


}
