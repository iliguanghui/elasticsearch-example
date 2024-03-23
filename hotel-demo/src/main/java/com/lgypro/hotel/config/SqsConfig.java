package com.lgypro.hotel.config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

@Configuration
public class SqsConfig {

    @Value("${sync.aws.sqs.region-id}")
    private String regionId;

    @Bean
    public SqsClient sqsClient() {
        return SqsClient.builder()
            .region(Region.of(regionId))
            .build();
    }

    @Bean
    public Gson gson() {
        return new GsonBuilder()
            .setPrettyPrinting()
            .serializeNulls()
            .create();
    }
}
