package com.example.lostfound.reliability;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class NotificationJobRepository {
    private final JdbcTemplate jdbc;

    public NotificationJobRepository(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    /** Insert once per event/channel; repeated delivery increments the observed attempt count. */
    public boolean recordCompletedJob(String eventId, String channel, String itemId, String eventType, int attempts) {
        int inserted = jdbc.update("""
                INSERT INTO notification_jobs(event_id, channel, item_id, event_type, status, attempt_count)
                VALUES (?, ?, ?, ?, 'COMPLETED', ?)
                ON CONFLICT (event_id, channel) DO NOTHING
                """, eventId, channel, itemId, eventType, attempts);
        if (inserted == 1) return true;
        jdbc.update("""
                UPDATE notification_jobs
                SET attempt_count = attempt_count + 1, updated_at = CURRENT_TIMESTAMP
                WHERE event_id = ? AND channel = ?
                """, eventId, channel);
        return false;
    }
}