package com.example.mqs.service;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import jakarta.annotation.PreDestroy;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.TimeoutException;

@Service
public class RabbitMQService implements iMessageQueueService{
    private final Connection connection;

    public RabbitMQService() throws IOException, TimeoutException{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        this.connection = factory.newConnection();
    }
    @Override
    public Channel subscribe(String exchangeName, String queueName) throws IOException {
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(exchangeName, "fanout");

        Map<String, Object> args = Map.of("x-queue-type", "quorum");
        channel.queueDeclare(queueName, true, false, false, args);
        channel.queueBind(queueName, exchangeName, "");

        System.out.println(LocalTime.now() + " [*] Waiting for messages.");

        return channel;
    }

    @Override
    public HttpStatusCode publish(String exchangeName, String message) {
        try (Channel channel = connection.createChannel()){
            channel.exchangeDeclare(exchangeName, "fanout");

            channel.basicPublish(exchangeName, "", MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());

            System.out.println(LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSS")) +
                    " [x] Published '" + message + "' in Exchange: " + exchangeName);

            return HttpStatus.ACCEPTED;

        } catch (IOException | TimeoutException e){
            System.out.println(e.getMessage());
            return HttpStatus.INTERNAL_SERVER_ERROR;
        }
    }

    @PreDestroy
    private void close() throws IOException {
        if (connection != null && connection.isOpen()){
            connection.close();
        }
    }
}
