package com.lgypro.hotel.service;

import com.google.gson.Gson;
import com.lgypro.hotel.pojo.HotelSyncMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import javax.annotation.PostConstruct;

@Service
@Slf4j
public class HotelSyncMessageSender {

    @Value("${sync.enable-produce-hotel-sync-messages}")
    private boolean enableProduceHotelSyncMessages;
    @Autowired
    private SqsClient sqsClient;

    @Value("${sync.aws.sqs.queue-name}")
    private String queueName;

    @Autowired
    private Gson gson;

    private String queueUrl;


    @PostConstruct
    public void init() {
        if (enableProduceHotelSyncMessages) {
            queueUrl = createQueue(queueName);
        }
    }

    public String createQueue(String queueName) {
        String queueUrl = null;
        try {
            GetQueueUrlResponse getQueueUrlResponse = sqsClient
                .getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            queueUrl = getQueueUrlResponse.queueUrl();
        } catch (QueueDoesNotExistException e) {
        }
        if (queueUrl == null) {
            sqsClient.createQueue(CreateQueueRequest.builder()
                .queueName(queueName)
                .build()
            );
            GetQueueUrlResponse getQueueUrlResponse = sqsClient
                .getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            queueUrl = getQueueUrlResponse.queueUrl();
        }
        return queueUrl;
    }

    public void sendMessage(HotelSyncMessage message) {
        String json = gson.toJson(message);
        sqsClient.sendMessage(SendMessageRequest.builder()
            .queueUrl(queueUrl)
            .messageBody(json)
            .build()
        );
    }
}
