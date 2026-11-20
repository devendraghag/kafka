# Lost & Found — Project 1

A small Spring Boot API publishes lost-item reports to Kafka. An audit listener logs each consumed record's key and broker metadata.

## Prerequisites

- Java 21
- Internet access for the Maven Wrapper and dependency downloads
- Docker Desktop with Docker Compose

## Run

Start the single-node KRaft broker and create the one-partition topic:

```powershell
docker compose up -d
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic lost-found.events --partitions 1 --replication-factor 1 --config retention.ms=604800000
```

Start the application in another terminal:

```powershell
.\mvnw.cmd spring-boot:run
```

Report a lost item:

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8080/lost-items -ContentType 'application/json' -Body '{"itemId":"wallet-42","description":"Brown leather wallet near the library"}'
```

The API returns HTTP 202 after Kafka acknowledges the send. `eventId` is a unique application event identifier; `itemId` is the Kafka record key. The broker assigns a partition and an offset within that partition. With one partition, all records are stored there; a key is the partitioning input and becomes more useful when the topic has multiple partitions. Offsets identify log positions, not event IDs.

## Inspect records and offsets

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic lost-found.events
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic lost-found.events --from-beginning --property print.key=true --property print.partition=true --property print.offset=true --property print.timestamp=true
```

The Spring audit consumer also logs key, topic, partition, offset, Kafka timestamp, and event body. To read one known partition/offset, use the consumer's `--partition 0 --offset N` options:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic lost-found.events --partition 0 --offset 0 --max-messages 1 --property print.key=true --property print.partition=true --property print.offset=true
```

## Retention experiment

The topic starts with a seven-day time retention. Shorten it to five seconds, publish an item, then wait and inspect again:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name lost-found.events --alter --add-config retention.ms=5000
```

Kafka removes expired log segments asynchronously; it does not promise deletion exactly five seconds after an individual record. For a quick repeatable experiment, stop the app, set `segment.ms` low along with retention, publish records, and inspect after segment rolling/cleanup. Restore the seven-day setting afterward:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name lost-found.events --alter --add-config retention.ms=604800000
```

Stop the broker with `docker compose down`; add `-v` only when you intentionally want to delete the broker's stored data.

## Maven Wrapper

Maven is pinned to 3.9.12 in this repository. On Windows, use the checked-in wrapper, which downloads and SHA-512 verifies Maven on first use; no global Maven installation is needed:

```powershell
.\mvnw.cmd spring-boot:run
```

Use `.\mvnw.cmd clean package` to build the jar. Java 21 and an internet connection for the first Maven/dependency downloads are still required.


## Postman

Import both files from `postman/` into Postman:

- `Lost-Found-Project-1.postman_collection.json`
- `Lost-Found-Local.postman_environment.json`

Select the `Lost & Found - Local` environment, make sure `baseUrl` is `http://localhost:8080`, then send **Report a lost item**. The API requires `itemId` and `description`; it generates `eventId` and `timestamp` itself. Start Kafka and the Spring Boot app first.

# Project 2 - Notifications and Event Schemas

Project 2 extends the same topic with two event types and Avro schemas:

- `POST /lost-items/v1` publishes the historical V1 Avro shape. Use it first for the schema-evolution demonstration.
- `POST /lost-items` publishes the current V2 `ItemReportedLost` schema. It adds optional `reportedLocation` with a null default.
- `POST /found-items` publishes an `ItemFound` event.

All events use `itemId` as the Kafka key. Email and SMS listeners are separate readers in `email-group` and `sms-group`; each group gets its own copy of every event. These listeners log a simulated notification. They do not connect to a real email or SMS provider.

The Project 1 topic may contain JSON-encoded history. Project 2 consumers use uto-offset-reset: latest, so newly created email and SMS groups begin with events published after they start and do not try to decode old JSON records as Avro. The existing audit group resumes from its committed position.

## Start Kafka, Schema Registry, and the topic

The Compose file exposes Kafka to your computer at `localhost:9092` and to containers at `kafka:29092`. Schema Registry is available at `http://localhost:8081`.

```powershell
docker compose up -d
```

The topic from Project 1 may already exist with one partition. Increase it to three:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic lost-found.events --partitions 3
```

If creating the topic for the first time, create it with three partitions and replication factor one. Check the result:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic lost-found.events
Invoke-RestMethod http://localhost:8081/subjects
```

Wait until Schema Registry answers before starting the application.

## Build and run

```powershell
.\mvnw.cmd -DskipTests clean package
.\mvnw.cmd spring-boot:run
```

The default app starts the audit, email, and SMS consumers. Open a second terminal for API requests.

## See fan-out and same-key ordering

Publish the V1 shape first, so Schema Registry sees the original schema before V2 adds the optional field:

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8080/lost-items/v1 -ContentType 'application/json' -Body '{"itemId":"wallet-42","description":"Brown leather wallet"}'
```

Then publish V2 for the same item, including the new optional field:

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8080/lost-items -ContentType 'application/json' -Body '{"itemId":"wallet-42","description":"Brown leather wallet","reportedLocation":"Near the library"}'
```

Finally publish an ItemFound event for the same key:

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8080/found-items -ContentType 'application/json' -Body '{"itemId":"wallet-42","foundLocation":"Library front desk"}'
```

Both email and SMS logs should show the V1, V2, and ItemFound records. Because the same key is used, the records go to the same partition and keep their order within that partition. With three partitions, different item IDs can be spread across the topic.

Schema Registry records the schemas by Avro record name. Inspect registered subjects and the versions of the evolved lost-event schema:

```powershell
Invoke-RestMethod http://localhost:8081/subjects
Invoke-RestMethod http://localhost:8081/subjects/com.example.lostfound.avro.ItemReportedLost/versions
```

`reportedLocation` is an Avro union of `null` and `string` with `default: null`. That default lets a V2 reader handle V1 data where the field is absent. The V1 endpoint is a learning aid; keep it ahead of V2 when creating a fresh Registry subject under the default backward-compatibility policy.

## Observe consumer group sharing and rebalancing

The first app process has one email consumer in `email-group`. Start a second PowerShell window in the project folder and run a second app instance with only the email listener enabled:

```powershell
$env:EMAIL_CONSUMER_ENABLED = 'true'
$env:SMS_CONSUMER_ENABLED = 'false'
$env:AUDIT_CONSUMER_ENABLED = 'false'
$env:KAFKA_CLIENT_ID = 'email-instance-2'
.\mvnw.cmd spring-boot:run '-Dspring-boot.run.arguments=--server.port=8082'
```

The second instance joins the same `email-group`. With three partitions, Kafka divides the partitions between the two email consumers and rebalances when the second instance joins. Stop it with Ctrl+C to see another rebalance.

While both are running, inspect lag and membership:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group email-group
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group email-group --members --verbose
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group sms-group
```

Compare the groups: `email-group` shares partitions between its two members; `sms-group` is separate and still receives every event independently.

## Postman

Import `postman/Lost-Found-Project-2.postman_collection.json` and `postman/Lost-Found-Local.postman_environment.json`. Select the local environment. The collection lists the V1 request first, then V2 and ItemFound, to make the schema evolution sequence easy to follow.



# Project 3 - Reliable Notification Delivery

See [docs/PROJECT-3.md](docs/PROJECT-3.md) for the retry and DLT demo, idempotent PostgreSQL notification jobs, record versus manual acknowledgement, producer idempotence, and the separate Kafka transaction exercise.
