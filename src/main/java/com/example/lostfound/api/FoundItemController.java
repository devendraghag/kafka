package com.example.lostfound.api;

import com.example.lostfound.avro.ItemFound;
import com.example.lostfound.messaging.ItemEventProducer;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/found-items")
public class FoundItemController {
    private final ItemEventProducer producer;
    public FoundItemController(ItemEventProducer producer) { this.producer = producer; }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, Object> reportFound(@Valid @RequestBody ReportFoundItemRequest request) throws Exception {
        ItemFound event = (ItemFound) producer.publishFound(request.itemId().trim(), request.foundLocation().trim())
                .get(10, TimeUnit.SECONDS).getProducerRecord().value();
        return Map.of("eventId", event.getEventId(), "itemId", event.getItemId(),
                "foundLocation", event.getFoundLocation(), "timestamp", event.getTimestamp());
    }

    public record ReportFoundItemRequest(@NotBlank String itemId, @NotBlank String foundLocation) { }
}
