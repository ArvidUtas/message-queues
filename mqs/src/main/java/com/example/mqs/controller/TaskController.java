package com.example.mqs.controller;

import com.example.mqs.service.iMessageQueueService;
import org.springframework.http.HttpStatusCode;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TaskController {
    private final iMessageQueueService mqs;
    public TaskController(iMessageQueueService mqs){
        this.mqs = mqs;
    }

    @GetMapping("/test")
    public HttpStatusCode test(@RequestParam String message){
        return mqs.publish("tasks", message);
    }
}
