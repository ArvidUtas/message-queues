package com.example.mqs.service;

import com.rabbitmq.client.Channel;
import org.springframework.http.HttpStatusCode;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

public interface iMessageQueueService {
    Channel subscribe(String channelName) throws IOException, TimeoutException;
    HttpStatusCode publish(String channelName, String message);
}
