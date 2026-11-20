package com.example.lostfound.reliability;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Qualifier;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaTransactionExercise {
    private static final Set<String> abortedOnce = ConcurrentHashMap.newKeySet();
    private final KafkaTemplate<String, Object> transactionalKafkaTemplate;
    private final String auditTopic;
    private final String emailTopic;

    public KafkaTransactionExercise(@Qualifier("transactionalKafkaTemplate") KafkaTemplate<String, Object> transactionalKafkaTemplate,
            @Value("${app.kafka.transaction-output-topic}") String auditTopic,
            @Value("${app.kafka.transaction-email-topic}") String emailTopic) {
        this.transactionalKafkaTemplate = transactionalKafkaTemplate;
        this.auditTopic = auditTopic;
        this.emailTopic = emailTopic;
    }

    @KafkaListener(id = "transactional-notification-exercise", topics = "${app.kafka.topic}",
            groupId = "transaction-demo-group", containerFactory = "transactionalListenerContainerFactory",
            autoStartup = "${app.notifications.transaction-demo.enabled:false}")
    public void fanOutAtomically(ConsumerRecord<String, SpecificRecord> consumed) {
        try {
            transactionalKafkaTemplate.send(auditTopic, consumed.key(), consumed.value()).get();
            transactionalKafkaTemplate.send(emailTopic, consumed.key(), consumed.value()).get();
            String detail = consumed.value() instanceof com.example.lostfound.avro.ItemReportedLost lost ? lost.getDescription().toString() : consumed.value() instanceof com.example.lostfound.avro.ItemFound found ? found.getFoundLocation().toString() : "";
            String id = consumed.value().getSchema().getName() + ":" + consumed.key();
            if (detail.contains("[TX_ABORT_ONCE]") && abortedOnce.add(id)) throw new IllegalStateException("Simulated failure after Kafka sends to demonstrate transaction abort");
        } catch (Exception exception) {
            throw new IllegalStateException("Transactional fan-out failed; source offset will not commit", exception);
        }
    }
}