package com.lgypro.hotel.service;

import com.google.gson.Gson;
import com.lgypro.hotel.pojo.HotelSyncMessage;
import com.lgypro.hotel.processor.HotelSyncMessageProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.Executor;

@Service
@Slf4j
public class HotelSyncMessageListener {

    @Value("${sync.enable-consume-hotel-sync-messages}")
    private boolean enableConsumeHotelSyncMessages;

    @Autowired
    private SqsClient sqsClient;

    @Value("${sync.aws.sqs.queue-name}")
    private String queueName;

    @Autowired
    private Gson gson;

    @Autowired
    private Executor executor;

    @Autowired
    private HotelSyncMessageProcessor messageProcessor;

    private String queueUrl;


    @PostConstruct
    public void init() {
        if (enableConsumeHotelSyncMessages) {
            GetQueueUrlResponse getQueueUrlResponse = sqsClient
                .getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            queueUrl = getQueueUrlResponse.queueUrl();
            consumerListener();
        }
    }

    public List<Message> receiveMessages() {
        return sqsClient.receiveMessage(ReceiveMessageRequest.builder()
            .queueUrl(queueUrl)
            .maxNumberOfMessages(10)
            .waitTimeSeconds(20)
            .build()
        ).messages();
    }

    public void deleteMessages(List<Message> messages) {
        messages.forEach(message -> {
            sqsClient.deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build());
        });
    }

    public void consumerListener() {
        executor.execute(() -> {
            while (true) {
                List<Message> messages = receiveMessages();
                messages.forEach(message -> {
                    String body = message.body();
                    HotelSyncMessage hotelSyncMessage = gson.fromJson(body, HotelSyncMessage.class);
                    messageProcessor.process(hotelSyncMessage);
                });
                deleteMessages(messages);
            }
        });
    }
}
