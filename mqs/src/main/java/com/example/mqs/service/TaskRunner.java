package com.example.mqs.service;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

@Service
public class TaskRunner {
    private final iMessageQueueService msq;
    private final ExecutorService executor = Executors.newFixedThreadPool(4);
    Channel channel;

    public TaskRunner(iMessageQueueService msq){
        this.msq = msq;
    }

    @PostConstruct
    public void start() throws IOException, TimeoutException{
        String channelName = "tasks";
        channel = msq.subscribe(channelName);
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            executor.submit(() -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

                System.out.println(" [x] Received '" + message + "'");

                String returnMessage = doWork(message);

                try {
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                System.out.println(" [x] Done: " + returnMessage);
            });
        };
        boolean autoAck = false;
        channel.basicConsume(channelName, autoAck, deliverCallback, consumerTag -> { });
    }

    private String doWork(String message){
        try{
            StringBuilder reverse = new StringBuilder();
            for (int i = message.length() - 1; i>=0; i-- ){
                reverse.append(message.charAt(i));
                Thread.sleep(i* 100L);
            }
            return reverse.toString();
        } catch (InterruptedException e){
            return e.getMessage();
        }
    }

    @PreDestroy
    public void close(){
        executor.shutdown();
    }
}
