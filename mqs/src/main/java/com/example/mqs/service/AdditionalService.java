package com.example.mqs.service;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeoutException;

@Service
public class AdditionalService {
    private final iMessageQueueService msq;
    private Channel channel;

    public AdditionalService(iMessageQueueService msq){
        this.msq = msq;
    }

    @PostConstruct
    public void start() throws IOException, TimeoutException {
        String exchangeName = "post";
        String channelName = "last";
        channel = msq.subscribe(exchangeName, channelName);
        DeliverCallback deliverCallback = (consumerTag, delivery)  -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

            System.out.println(LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSS")) +
                    " [x] Received in last service '" + message + "'");
            try {
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } catch (IOException e) {
                System.err.println(" [ ] Failed in last service: '" + message + "'. Error: '" + e.getMessage() + "'");
            }
        };
        boolean autoAck = false;
        channel.basicConsume(channelName, autoAck, deliverCallback, consumerTag -> { });
    }
}
