package com.example.lostfound.messaging;

import com.example.lostfound.reliability.EmailNotificationProcessor;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class NotificationConsumers {
    private static final Logger log = LoggerFactory.getLogger(NotificationConsumers.class);
    private final EmailNotificationProcessor emailProcessor;
    private final String ackMode;

    public NotificationConsumers(EmailNotificationProcessor emailProcessor,
                                 @Value("${app.notifications.email.ack-mode:record}") String ackMode) {
        this.emailProcessor = emailProcessor;
        this.ackMode = ackMode;
    }

    @KafkaListener(id = "email-notification-listener", topics = "${app.kafka.topic}", groupId = "email-group",
            containerFactory = "emailRetryListenerContainerFactory",
            autoStartup = "${app.notifications.email.enabled:true}")
    public void sendEmail(ConsumerRecord<String, SpecificRecord> record, Acknowledgment acknowledgment) {
        emailProcessor.process(record);
        if ("manual_immediate".equalsIgnoreCase(ackMode) || "manual".equalsIgnoreCase(ackMode)) {
            acknowledgment.acknowledge();
        }
    }

    @KafkaListener(id = "sms-notification-listener", topics = "${app.kafka.topic}", groupId = "sms-group",
            autoStartup = "${app.notifications.sms.enabled:true}")
    public void sendSms(ConsumerRecord<String, SpecificRecord> record) {
        log.info("SMS would notify: eventType={} key={} topic={} partition={} offset={} event={}",
                record.value().getSchema().getName(), record.key(), record.topic(), record.partition(), record.offset(), record.value());
    }
}