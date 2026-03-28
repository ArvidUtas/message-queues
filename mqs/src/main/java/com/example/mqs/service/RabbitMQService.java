package com.example.mqs.service;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import jakarta.annotation.PreDestroy;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Service;

import java.io.IOException;
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
    public Channel subscribe(String channelName) throws IOException {
        Channel channel = connection.createChannel();
        Map<String, Object> args = Map.of("x-queue-type", "quorum");
        channel.queueDeclare(channelName, true, false, false, args);
        System.out.println(" [*] Waiting for messages.");
        return channel;
    }

    @Override
    public HttpStatusCode publish(String channelName, String message) {
        try (Channel channel = connection.createChannel()){

            Map<String, Object> args = Map.of("x-queue-type", "quorum");
            channel.queueDeclare(channelName, true, false, false, args);

            channel.basicPublish("", channelName, null, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");

            return HttpStatus.OK;

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
