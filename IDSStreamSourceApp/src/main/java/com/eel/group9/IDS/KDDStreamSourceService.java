package com.eel.group9.IDS;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;

@Component
public class KDDStreamSourceService {

    private static final String TOPIC = "streamIn27A";
    private KafkaTemplate kafkaTemplate;
    private ResourceLoader resourceLoader;

    @Autowired
    public KDDStreamSourceService(KafkaTemplate kafkaTemplate, ResourceLoader resourceLoader) {
        this.kafkaTemplate = kafkaTemplate;
        this.resourceLoader = resourceLoader;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void streamCSVToKafka() {
        int count = 0;
        long startTime = System.currentTimeMillis();
        Resource resource = resourceLoader.getResource("classpath:streamingInputData.csv");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(resource.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                kafkaTemplate.send(TOPIC, line);
                count++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        long endTime = System.currentTimeMillis();
        long time = (endTime - startTime);
        System.out.println("Took " + time + " ms to publish " + count + " records");

    }
}
