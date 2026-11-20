package com.example.lostfound.messaging;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ItemAuditConsumer {
    private static final Logger log = LoggerFactory.getLogger(ItemAuditConsumer.class);

    @KafkaListener(id = "audit-listener", topics = "${app.kafka.topic}", groupId = "lost-found-audit-group",
            autoStartup = "${app.notifications.audit.enabled:true}")
    public void audit(ConsumerRecord<String, SpecificRecord> record) {
        log.info("AUDIT key={} topic={} partition={} offset={} kafkaTimestamp={} eventType={} event={}",
                record.key(), record.topic(), record.partition(), record.offset(), record.timestamp(),
                record.value().getSchema().getName(), record.value());
    }
}
