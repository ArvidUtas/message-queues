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
public class LoggerService {
    private final iMessageQueueService msq;
    private Channel channel;

    public LoggerService(iMessageQueueService msq){
        this.msq = msq;
    }

    @PostConstruct
    public void start() throws IOException, TimeoutException {
        String exchangeName = "main";
        String channelName = "logg";
        channel = msq.subscribe(exchangeName, channelName);
        DeliverCallback deliverCallback = (consumerTag, delivery)  -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

            System.out.println(LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSS")) +
                    " [x] Received in logger '" + message + "'");
            try {
                loggMessage(message);

                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

                System.out.println(LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSS")) + " [x] Done logging: '" + message + "'");
            } catch (IOException | InterruptedException e) {
                System.err.println(LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSS")) +
                        " [ ] Fail logging: '" + message + "'. Error: '" + e.getMessage() + "'");
            }
        };
        boolean autoAck = false;
        channel.basicConsume(channelName, autoAck, deliverCallback, consumerTag -> { });
    }

    private void loggMessage(String message) throws InterruptedException{
        Thread.sleep(1000L);
        System.out.println(LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSS")) +
                " [x] Logging is important. This important message: '" + message + "' has been logged. " +
                "Not really I'm just trying out fanout");
    }
}
