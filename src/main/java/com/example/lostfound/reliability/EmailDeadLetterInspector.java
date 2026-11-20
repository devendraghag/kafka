package com.example.lostfound.reliability;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Component
public class EmailDeadLetterInspector {
    private static final Logger log = LoggerFactory.getLogger(EmailDeadLetterInspector.class);

    @KafkaListener(id = "email-dlt-inspector", topics = "${app.kafka.email-dlt}", groupId = "email-dlt-inspector-group",
            autoStartup = "${EMAIL_DLT_INSPECTOR_ENABLED:false}", properties = "auto.offset.reset=earliest")
    public void inspect(ConsumerRecord<String, SpecificRecord> record) {
        String headers = StreamSupport.stream(record.headers().spliterator(), false)
                .map(EmailDeadLetterInspector::formatHeader).collect(Collectors.joining(", "));
        log.error("EMAIL DLT record key={} partition={} offset={} event={} headers=[{}]",
                record.key(), record.partition(), record.offset(), record.value(), headers);
    }

    private static String formatHeader(Header header) {
        return header.key() + "=" + new String(header.value(), StandardCharsets.UTF_8).replaceAll("[\\r\\n]+", " ");
    }
}