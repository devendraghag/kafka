package com.example.lostfound.messaging;

import com.example.lostfound.avro.ItemFound;
import com.example.lostfound.avro.ItemReportedLost;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Component
public class ItemEventProducer {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic;
    private final Schema lostV1Schema;

    public ItemEventProducer(KafkaTemplate<String, Object> kafkaTemplate, @Value("${app.kafka.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        try (var stream = new ClassPathResource("avro/v1/ItemReportedLost.avsc").getInputStream()) {
            this.lostV1Schema = new Schema.Parser().parse(stream);
        } catch (IOException exception) {
            throw new UncheckedIOException("Could not load the historical ItemReportedLost v1 schema", exception);
        }
    }

    public CompletableFuture<SendResult<String, Object>> publishLost(String itemId, String description, String reportedLocation) {
        var event = ItemReportedLost.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setItemId(itemId)
                .setDescription(description)
                .setTimestamp(java.time.Instant.now())
                .setReportedLocation(reportedLocation)
                .build();
        return kafkaTemplate.send(topic, itemId, event);
    }

    public CompletableFuture<SendResult<String, Object>> publishLostV1(String eventId, String itemId,
                                                                       String description, long timestamp) {
        GenericRecord event = new GenericData.Record(lostV1Schema);
        event.put("eventId", eventId);
        event.put("itemId", itemId);
        event.put("description", description);
        event.put("timestamp", timestamp);
        return kafkaTemplate.send(topic, itemId, event);
    }

    public CompletableFuture<SendResult<String, Object>> publishFound(String itemId, String foundLocation) {
        var event = ItemFound.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setItemId(itemId)
                .setFoundLocation(foundLocation)
                .setTimestamp(java.time.Instant.now())
                .build();
        return kafkaTemplate.send(topic, itemId, event);
    }
}
