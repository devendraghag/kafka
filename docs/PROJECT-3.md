# Project 3 - Reliable Notification Delivery

Project 3 adds a simulated email delivery pipeline to Project 2. Email failures are retried twice after the first attempt (three total attempts) with a 500 ms pause. A record that still fails is sent to `email.notifications.DLT`. PostgreSQL stores one notification job per `(event_id, channel)`, which makes handling the same event again safe. Email remains simulated: the durable job row represents the completed exercise side effect; no external email provider is called.

## Start the services

From the repository root, start Kafka, Schema Registry, and PostgreSQL, then start the app:

```powershell
docker compose up -d
.\mvnw.cmd spring-boot:run
```

The app creates the DLT and transaction output topics at startup. PostgreSQL creates `notification_jobs` from `src/main/resources/schema.sql`. Its Docker volume preserves the table between container restarts.

## Retry once, then succeed

Send a lost item whose description contains `[FAIL_ONCE]`. The email consumer throws on its first delivery, then the retry succeeds and writes one job row. The SMS and audit consumers continue independently.

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8080/lost-items -ContentType 'application/json' -Body '{"itemId":"wallet-retry","description":"Wallet [FAIL_ONCE]"}'
```

The email app logs a simulated failure and retry, then `EMAIL job persisted`. Check the saved job:

```powershell
docker compose exec postgres psql -U lost_found -d lost_found -c "SELECT event_id, channel, item_id, event_type, status, created_at FROM notification_jobs ORDER BY id DESC LIMIT 10;"
```

## Fail every attempt and inspect the DLT

Send an event with `[FAIL_ALWAYS]`. The email listener makes three attempts, then the error handler publishes the original Avro record to the DLT and adds failure metadata headers. Later valid records continue after recovery.

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8080/lost-items -ContentType 'application/json' -Body '{"itemId":"wallet-dead","description":"Wallet [FAIL_ALWAYS]"}'
Invoke-RestMethod -Method Post -Uri http://localhost:8080/lost-items -ContentType 'application/json' -Body '{"itemId":"wallet-ok","description":"Valid later event"}'
```

To print the full Avro record and DLT headers, open another PowerShell window, disable the normal consumers, enable the inspector, and start a second app instance:

```powershell
$env:EMAIL_CONSUMER_ENABLED = 'false'
$env:SMS_CONSUMER_ENABLED = 'false'
$env:AUDIT_CONSUMER_ENABLED = 'false'
$env:EMAIL_DLT_INSPECTOR_ENABLED = 'true'
$env:KAFKA_CLIENT_ID = 'dlt-inspector'
.\mvnw.cmd spring-boot:run '-Dspring-boot.run.arguments=--server.port=8082'
```

The DLT inspector starts from the beginning when its group has no committed offsets. Its log includes the original key/value, topic partition and offset, plus headers such as the exception class, message, stack trace and original record coordinates.

## Duplicate-safe database work and the crash window

Publish an event whose description contains `[CRASH_AFTER_DB_COMMIT]`:

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8080/lost-items -ContentType 'application/json' -Body '{"itemId":"wallet-crash","description":"Wallet [CRASH_AFTER_DB_COMMIT]"}'
```

The consumer inserts the unique `(event_id, EMAIL)` row and then deliberately throws before its listener returns, so the offset is not acknowledged. Kafka redelivers the record. The second database insert hits `ON CONFLICT DO NOTHING`; the processor logs `EMAIL duplicate delivery ignored`, then returns successfully. Check that the event has only one row:

```powershell
docker compose exec postgres psql -U lost_found -d lost_found -c "SELECT event_id, channel, COUNT(*) FROM notification_jobs GROUP BY event_id, channel HAVING event_id = '<event-id-from-response>';"
```

The database side effect is safe to repeat because of the unique constraint and insert behavior. The simulation's failure markers live in process memory; restart the app between exercises if you want a clean one-time failure.

## Automatic record commit versus manual acknowledgement

`enable.auto.commit` is disabled. With the default `KAFKA_ACK_MODE=record`, Spring commits the record after the listener returns successfully. For the manual exercise, stop the app, then start it with:

```powershell
$env:KAFKA_ACK_MODE = 'manual_immediate'
.\mvnw.cmd spring-boot:run
```

The listener now explicitly acknowledges only after the database processor succeeds. A thrown exception skips the acknowledgement and the error handler retries or dead-letters the record. Stop and restart without that variable to return to record mode. Both examples demonstrate at-least-once processing: a record may be delivered more than once, so database work must be idempotent.

Exactly-once Kafka processing can atomically coordinate Kafka output records with Kafka consumer offsets. It cannot make an external PostgreSQL transaction part of that same Kafka transaction. For Kafka plus a database, use idempotent database writes (or an outbox/inbox design) and accept/reconcile redelivery.

## Producer idempotence

The regular producer uses `acks=all`, `enable.idempotence=true`, retries, and a safe in-flight request limit. This prevents duplicate Kafka appends caused by producer retries within a producer session. It does not deduplicate a caller that submits the same HTTP operation twice; application-level idempotency still needs an event/request ID policy.

## Kafka transaction exercise

This separate listener consumes the source topic as `transaction-demo-group`, then sends each record to both `notification.transactional.audit` and `notification.transactional.email`. Its container starts a Kafka transaction; Spring sends the consumed offset to the same transaction when the listener returns. The two topic writes and source offset therefore commit together. It is disabled by default so it does not compete with the regular exercises.

Stop the app and start it with the transaction demo enabled:

```powershell
$env:KAFKA_TRANSACTION_DEMO_ENABLED = 'true'
.\mvnw.cmd spring-boot:run
```

Publish a normal lost-item event. In a second window, read each output topic with `read_committed` isolation:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic notification.transactional.audit --from-beginning --isolation-level read_committed --max-messages 1 --property print.key=true
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic notification.transactional.email --from-beginning --isolation-level read_committed --max-messages 1 --property print.key=true
```

To demonstrate abort and redelivery, publish a description containing `[TX_ABORT_ONCE]`. The listener sends to both output topics and deliberately throws once before returning. The transaction aborts, so a `read_committed` consumer sees neither partial output; the source offset is not committed and the listener receives the event again. The retry succeeds and both output records become visible together. Turn the demo off by stopping the app and clearing the environment variable:

```powershell
Remove-Item Env:KAFKA_TRANSACTION_DEMO_ENABLED
```

Kafka transactions cover Kafka topics and Kafka offsets only. They do not include the PostgreSQL `notification_jobs` insert or an actual email provider.