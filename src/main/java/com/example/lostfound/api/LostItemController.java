package com.example.lostfound.api;

import com.example.lostfound.avro.ItemReportedLost;
import com.example.lostfound.messaging.ItemEventProducer;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/lost-items")
public class LostItemController {
    private final ItemEventProducer producer;
    public LostItemController(ItemEventProducer producer) { this.producer = producer; }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, Object> report(@Valid @RequestBody ReportLostItemRequest request) throws Exception {
        String itemId = request.itemId().trim();
        ItemReportedLost event = (ItemReportedLost) producer.publishLost(itemId, request.description().trim(), trimToNull(request.reportedLocation()))
                .get(10, TimeUnit.SECONDS).getProducerRecord().value();
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("eventId", event.getEventId());
        response.put("itemId", event.getItemId());
        response.put("description", event.getDescription());
        response.put("timestamp", event.getTimestamp());
        response.put("reportedLocation", event.getReportedLocation());
        return response;
    }

    @PostMapping("/v1")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, Object> reportUsingV1Schema(@Valid @RequestBody ReportLostItemRequest request) throws Exception {
        String eventId = UUID.randomUUID().toString();
        String itemId = request.itemId().trim();
        String description = request.description().trim();
        long timestamp = System.currentTimeMillis();
        producer.publishLostV1(eventId, itemId, description, timestamp).get(10, TimeUnit.SECONDS);
        return Map.of("eventId", eventId, "itemId", itemId, "description", description,
                "timestamp", Instant.ofEpochMilli(timestamp));
    }

    private String trimToNull(String value) { return value == null || value.isBlank() ? null : value.trim(); }
    public record ReportLostItemRequest(@NotBlank String itemId, @NotBlank String description, String reportedLocation) { }
}
