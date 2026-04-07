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
        DeliverCallback deliverCallback = (consumerTag, delivery) -> executor.submit(() -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

            System.out.println(" [x] Received '" + message + "'");

            try {
                String returnMessage = doWork(message);

                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

                System.out.println(" [x] Done: '" + returnMessage + "'");
            } catch (IOException | InterruptedException e) {
                System.err.println(" [ ] Fail: '" + message + "'. Error: '" + e.getMessage() + "'");
            }
        });
        boolean autoAck = false;
        channel.basicConsume(channelName, autoAck, deliverCallback, consumerTag -> { });
    }

    private String doWork(String message) throws InterruptedException {
        StringBuilder reverse = new StringBuilder();
        for (int i = message.length() - 1; i>=0; i-- ){
            reverse.append(message.charAt(i));
            Thread.sleep(i* 100L);
        }
        return reverse.toString();
    }

    @PreDestroy
    public void close(){
        executor.shutdown();
    }
}
