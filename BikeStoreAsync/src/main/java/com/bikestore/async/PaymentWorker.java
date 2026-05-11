package com.bikestore.async;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class PaymentWorker {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        Map<String, Object> argsQueue = new HashMap<>();
        channel.queueDeclare(
                RabbitConfig.ORDER_QUEUE,
                true,
                false,
                false,
                null
        );

        channel.queueDeclare(
                RabbitConfig.DLQ,
                true,
                false,
                false,
                null
        );

        System.out.println("=================================");
        System.out.println("PAYMENT WORKER ESPERANDO");
        System.out.println("=================================");

        DeliverCallback callback = (consumerTag, delivery) -> {

            String message = new String(delivery.getBody());

            JsonNode json = mapper.readTree(message);

            String pedidoId = json.get("pedidoId").asText();

            boolean success = new Random().nextBoolean();

            int retries = 0;

            if(delivery.getProperties().getHeaders() != null &&
                    delivery.getProperties().getHeaders().containsKey("x-retries")){

                retries = (int) delivery.getProperties()
                        .getHeaders()
                        .get("x-retries");
            }

            System.out.println("=================================");
            System.out.println("THREAD: " + Thread.currentThread().getName());
            System.out.println("TIME: " + LocalDateTime.now());
            System.out.println("Pedido: " + pedidoId);
            System.out.println("Retries: " + retries);

            if(success){

                System.out.println("PAYMENT STATUS: PAID");

                EmailWorker.sendEmail(pedidoId);

            }else{

                System.out.println("PAYMENT STATUS: FAILED");

                if(retries < 3){

                    Map<String, Object> headers = new HashMap<>();
                    headers.put("x-retries", retries + 1);

                    AMQP.BasicProperties properties =
                            new AMQP.BasicProperties
                                    .Builder()
                                    .headers(headers)
                                    .build();

                    channel.basicPublish(
                            "",
                            RabbitConfig.ORDER_QUEUE,
                            properties,
                            message.getBytes()
                    );

                    System.out.println("REENCOLANDO MENSAJE");

                }else{

                    channel.basicPublish(
                            "",
                            RabbitConfig.DLQ,
                            null,
                            message.getBytes()
                    );

                    System.out.println("ENVIADO A DEAD LETTER QUEUE");

                }

            }

            System.out.println("=================================");

        };

        channel.basicConsume(
                RabbitConfig.ORDER_QUEUE,
                true,
                callback,
                consumerTag -> {}
        );
    }
}