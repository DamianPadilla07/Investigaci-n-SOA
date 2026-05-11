package com.bikestore.async;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class OrderProducer {

    public static void main(String[] args) throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(
                RabbitConfig.ORDER_QUEUE,
                true,
                false,
                false,
                null
        );

        String json = """
                {
                    "pedidoId":"ORD-1001",
                    "cliente":"Damian",
                    "paymentStatus":"PENDING"
                }
                """;

        channel.basicPublish(
                "",
                RabbitConfig.ORDER_QUEUE,
                null,
                json.getBytes()
        );

        System.out.println("=================================");
        System.out.println("PEDIDO ENVIADO");
        System.out.println(json);
        System.out.println("=================================");

        channel.close();
        connection.close();
    }
}