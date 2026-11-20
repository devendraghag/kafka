package com.example.lostfound.reliability;

import com.example.lostfound.avro.ItemFound;
import com.example.lostfound.avro.ItemReportedLost;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.util.Set;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class EmailNotificationProcessor {
    private static final Logger log = LoggerFactory.getLogger(EmailNotificationProcessor.class);
    private final NotificationJobRepository jobs;
    private final Set<String> failedOnce = ConcurrentHashMap.newKeySet();
    private final Set<String> crashedAfterCommit = ConcurrentHashMap.newKeySet();
    private final Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();

    public EmailNotificationProcessor(NotificationJobRepository jobs) { this.jobs = jobs; }

    public void process(ConsumerRecord<String, SpecificRecord> record) {
        SpecificRecord event = record.value();
        String eventId = stringField(event, "eventId");
        String itemId = stringField(event, "itemId");
        String eventType = event.getSchema().getName();
        String detail = detail(event);
        String identity = eventId + ":email";
        int attempt = attempts.computeIfAbsent(identity, ignored -> new AtomicInteger()).incrementAndGet();
        if (detail.contains("[FAIL_ALWAYS]")) {
            throw new IllegalStateException("Simulated permanent email provider failure for " + eventId);
        }
        if (detail.contains("[FAIL_ONCE]") && failedOnce.add(identity)) {
            throw new IllegalStateException("Simulated one-time email provider failure for " + eventId);
        }
        boolean inserted = jobs.recordCompletedJob(eventId, "EMAIL", itemId, eventType, attempt);
        if (detail.contains("[CRASH_AFTER_DB_COMMIT]") && crashedAfterCommit.add(identity)) {
            throw new SimulatedPostCommitCrashException("Simulated crash after durable job insert for " + eventId);
        }
        if (inserted) log.info("EMAIL job persisted eventId={} itemId={} eventType={}", eventId, itemId, eventType);
        else log.info("EMAIL duplicate delivery ignored eventId={} itemId={}", eventId, itemId);
    }

    private static String detail(SpecificRecord event) {
        if (event instanceof ItemReportedLost lost) return lost.getDescription().toString();
        if (event instanceof ItemFound found) return found.getFoundLocation().toString();
        return "";
    }

    private static String stringField(SpecificRecord event, String field) {
        Object value = event.get(event.getSchema().getField(field).pos());
        return value == null ? "" : value.toString();
    }

    public static class SimulatedPostCommitCrashException extends RuntimeException {
        public SimulatedPostCommitCrashException(String message) { super(message); }
    }
}