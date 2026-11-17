# Kafka + Avro: Order Service and Inventory Service

## 1. Project Overview

This project demonstrates asynchronous communication between two Spring Boot microservices using **Apache Kafka, Apache Avro, and Confluent Schema Registry**.

The two services are:

- **Order Service:** Creates orders and publishes order events.
- **Inventory Service:** Consumes order events, processes inventory, and publishes inventory response events.

The goal is to understand how independent microservices communicate through Kafka without directly depending on each other's HTTP APIs.

## 2. Technologies Used

- Java
- Spring Boot
- Apache Kafka
- Apache Avro
- Confluent Schema Registry
- Docker and Docker Compose
- Maven

## 3. Architecture

Initially, the communication flow was one-directional:

1. Order Service creates an order.
2. Order Service publishes an `OrderCreatedEvent`.
3. Inventory Service consumes the event.

The architecture is extended to support communication in both directions.

### Bidirectional event communication

```text
              ORDER SERVICE
                    |
                    |
          Creates OrderCreatedEvent
                    |
                    v
          order-created-topic
                    |
                    v
             INVENTORY SERVICE
                    |
              Consumes Event
                    |
             Business Logic
                    |
             Updates Stock
                    |
          Creates InventoryUpdatedEvent
                    |
                    v
         inventory-updated-topic
                    |
                    v
              ORDER SERVICE
                    |
              Consumes Event
                    |
            Updates Order Status
```

The important concept is that **a service can be both a Kafka producer and a Kafka consumer**.

## 4. Kafka and Schema Registry Setup

Kafka and Schema Registry are run using Docker.

The project uses these Confluent images:

- Kafka: `confluentinc/cp-kafka:7.7.1`
- Schema Registry: `confluentinc/cp-schema-registry:7.7.1`

Local application connection details:

| Service | Address |
|---|---|
| Kafka | `localhost:9092` |
| Schema Registry | `http://localhost:8081` |

For applications running inside the same Docker network, use the corresponding Docker service names and internal ports instead.

### Useful Docker commands

Start services:

```powershell
docker compose up -d
```

Check running containers:

```powershell
docker ps
```

Start Schema Registry separately:

```powershell
docker compose up -d schema-registry
```

Stop services:

```powershell
docker compose down
```

Check Schema Registry connectivity:

```powershell
Test-NetConnection localhost -Port 8081
```

## 5. Kafka Topics

Two topics are used for communication:

| Topic | Purpose |
|---|---|
| `order-created-topic` | Carries events from Order Service to Inventory Service |
| `inventory-updated-topic` | Carries inventory response events to Order Service |

Inspect a topic:

```powershell
docker exec broker kafka-topics --bootstrap-server broker:29092 --describe --topic order-created-topic
```

Inspect topic offsets:

```powershell
docker exec broker kafka-get-offsets --bootstrap-server broker:29092 --topic order-created-topic
```

An increasing end offset indicates that new records have been appended to the topic.

## 6. Avro Schema and Java Class Generation

Avro defines the structure of an event.

For example, the `OrderCreatedEvent` schema defines fields such as:

- `orderId`
- `orderStatus`
- `orderTotal`
- `quantity`

Example schema:

```json
{
  "type": "record",
  "name": "OrderCreatedEvent",
  "namespace": "com.homework.learnkafka",
  "fields": [
    {
      "name": "orderId",
      "type": "string"
    },
    {
      "name": "orderStatus",
      "type": "string"
    },
    {
      "name": "orderTotal",
      "type": "double"
    },
    {
      "name": "quantity",
      "type": "int"
    }
  ]
}
```

The Avro Maven plugin generates Java classes from `.avsc` files.

Example Maven command:

```powershell
mvn clean generate-sources
```

Generated Java classes can then be imported into the application.

### Schema Registry

Schema Registry manages and stores Avro schemas.

It allows producers and consumers to work with registered schemas and supports schema compatibility checks.

Important distinction:

- Avro Maven plugin generates Java classes.
- Schema Registry manages registered schemas.
- Kafka transports the serialized event records.

## 7. Maven Dependencies and Version Alignment

The project uses:

- `spring-boot-starter-kafka`
- `io.confluent:kafka-avro-serializer`
- `org.apache.avro:avro`
- `avro-maven-plugin`

The Confluent serializer and Schema Registry versions were aligned to `7.7.1`, matching the Docker Confluent components used in this project.

Example version property:

```xml
<properties>
    <avro.version>1.12.0</avro.version>
    <confluent.version>7.7.1</confluent.version>
</properties>
```

The Avro Maven plugin runs during the `generate-sources` phase.

## 8. Order Service Configuration

Order Service acts as:

1. Kafka producer for `OrderCreatedEvent`.
2. Kafka consumer for `InventoryUpdatedEvent`.

Its `application.yml`:

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092

    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
      properties:
        schema.registry.url: http://127.0.0.1:8081

    consumer:
      group-id: order-group
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: io.confluent.kafka.serializers.KafkaAvroDeserializer
      properties:
        schema.registry.url: http://127.0.0.1:8081
        specific.avro.reader: true
```

### Producer

Order Service creates and publishes an Avro event:

```java
kafkaTemplate.send("order-created-topic", orderCreatedEvent);
```

### Consumer

Order Service listens for inventory response events:

```java
@KafkaListener(
    topics = "inventory-updated-topic",
    groupId = "order-group"
)
public void listen(InventoryUpdatedEvent event) {

    System.out.println("Order ID: " + event.getOrderId());
    System.out.println("Inventory Status: " + event.getStatus());

    // Update order status in the database.
}
```

## 9. Inventory Service Configuration

Inventory Service also acts as both a producer and consumer.

Its `application.yml`:

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092

    consumer:
      group-id: inventory-group
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: io.confluent.kafka.serializers.KafkaAvroDeserializer
      properties:
        schema.registry.url: http://127.0.0.1:8081
        specific.avro.reader: true

    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
      properties:
        schema.registry.url: http://127.0.0.1:8081
```

### Consumer

Inventory Service receives an order event:

```java
@KafkaListener(
    topics = "order-created-topic",
    groupId = "inventory-group"
)
public void listen(OrderCreatedEvent event) {

    System.out.println("Message received from Kafka");
    System.out.println("Order ID: " + event.getOrderId());
    System.out.println("Order Status: " + event.getOrderStatus());
    System.out.println("Order Total: " + event.getOrderTotal());
    System.out.println("Quantity: " + event.getQuantity());
}
```

The listener receives the generated Avro Java object because `specific.avro.reader` is enabled.

## 10. Producing a New Event After Consuming an Event

This is an important concept learned while extending the project.

**A service can receive one event, execute business logic, create a different event, and publish that new event.**

For example:

1. Inventory Service receives `OrderCreatedEvent`.
2. It checks whether the requested quantity is available.
3. It updates inventory if sufficient stock exists.
4. It creates a new `InventoryUpdatedEvent`.
5. It publishes that event to `inventory-updated-topic`.
6. Order Service consumes the response.

### Why create a different event?

`OrderCreatedEvent` represents order creation.

`InventoryUpdatedEvent` represents the result of inventory processing.

Using separate event types makes the communication clearer and keeps each event focused on a particular business occurrence.

### InventoryUpdatedEvent schema

Create `InventoryUpdatedEvent.avsc`:

```json
{
  "type": "record",
  "name": "InventoryUpdatedEvent",
  "namespace": "com.homework.learnkafka",
  "fields": [
    {
      "name": "orderId",
      "type": "string"
    },
    {
      "name": "status",
      "type": "string"
    },
    {
      "name": "message",
      "type": "string"
    }
  ]
}
```

Generate the corresponding Java class using the Avro Maven plugin.

### Inventory Service producer

After processing the order, create the new event:

```java
InventoryUpdatedEvent response =
    InventoryUpdatedEvent.newBuilder()
        .setOrderId(event.getOrderId())
        .setStatus("SUCCESS")
        .setMessage("Inventory updated successfully")
        .build();

kafkaTemplate.send(
    "inventory-updated-topic",
    response
);
```

The example assumes the inventory update succeeded. In actual business logic, publish `SUCCESS` only after the stock update has completed successfully. If stock is insufficient, publish a failure response instead.

### Order Service consumer

Order Service receives the new event:

```java
@KafkaListener(
    topics = "inventory-updated-topic",
    groupId = "order-group"
)
public void listen(InventoryUpdatedEvent event) {

    System.out.println("Order ID: " + event.getOrderId());
    System.out.println("Status: " + event.getStatus());
    System.out.println("Message: " + event.getMessage());

    // Update order status in the database.
}
```

### Important concepts

| Component | Responsibility |
|---|---|
| `@KafkaListener` | Receives Kafka messages |
| Business logic | Processes the received event |
| `newBuilder()` | Creates a new Avro event object |
| `KafkaTemplate.send()` | Publishes the new event |
| Kafka topic | Carries the event to interested consumers |

This is asynchronous event-driven communication. It is not a synchronous HTTP request-response operation.

Use `orderId` to correlate the response with the original order. Also design consumers to handle duplicate events safely.

## 11. Do Both Services Need Avro Schema Files?

Both services need access to the schemas for the event types they produce or consume.

| Schema | Order Service | Inventory Service |
|---|---|---|
| `OrderCreatedEvent.avsc` | Produces | Consumes |
| `InventoryUpdatedEvent.avsc` | Consumes | Produces |

### Option 1: Keep schemas in both services

```text
order-service/
    src/main/resources/avro/
        OrderCreatedEvent.avsc
        InventoryUpdatedEvent.avsc

inventory-service/
    src/main/resources/avro/
        OrderCreatedEvent.avsc
        InventoryUpdatedEvent.avsc
```

Each service generates its own Java classes.

This is straightforward for a learning project.

### Option 2: Create a shared event-schema module

For a larger project, a separate Maven module can hold the event schemas and generated classes:

```text
project/
    kafka-events/
        src/main/avro/
            OrderCreatedEvent.avsc
            InventoryUpdatedEvent.avsc

    order-service/
    inventory-service/
```

Both services include `kafka-events` as a Maven dependency.

This avoids manually maintaining duplicate schema files.

## 12. Problems Encountered and Their Solutions

### Problem 1: Kafka port 9092 already in use

Docker Kafka failed to start because another Kafka broker was already running locally.

Find the process:

```powershell
Get-NetTCPConnection -LocalPort 9092 -State Listen |
Select-Object LocalAddress, LocalPort, OwningProcess
```

Inspect the process:

```powershell
Get-Process -Id 22728
```

The conflicting process was a local Kafka installation under:

```text
C:\kafka
```

Stop the local broker gracefully before starting the Docker broker.

### Problem 2: Schema Registry connection refused

The application could not connect to Schema Registry.

Check connectivity:

```powershell
Test-NetConnection localhost -Port 8081
```

Start Schema Registry:

```powershell
docker compose up -d schema-registry
```

### Problem 3: Confluent version mismatch

The application initially used Confluent version `8.1.6`, while the Docker Schema Registry was using `7.7.1`.

The Confluent dependency version was aligned to:

```xml
<confluent.version>7.7.1</confluent.version>
```

The user confirmed that the application worked after this alignment.

### Problem 4: String deserialization error

Error:

```text
Cannot convert from
[com.homework.learnkafka.OrderCreatedEvent]
to [java.lang.String]
```

Cause:

The consumer was expecting a String, but Kafka Avro Deserializer was returning an Avro-generated Java object.

Solution:

Change the listener parameter to:

```java
public void listen(OrderCreatedEvent message)
```

### Problem 5: String serializer error

Error:

```text
OrderCreatedEvent cannot be cast to java.lang.String
```

Cause:

The producer was using `StringSerializer` for the message value, but the application was sending an Avro object.

Solution:

```yaml
producer:
  key-serializer: org.apache.kafka.common.serialization.StringSerializer
  value-serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
```

The key serializer and value serializer have different responsibilities.

### Problem 6: Avro serialization NullPointerException

Error:

```text
java.lang.NullPointerException:
Cannot invoke "Object.getClass()" because "datum" is null
```

Cause:

At least one required String field in the Avro record being serialized was null.

Solution:

Check all required fields before publishing the event.

For example:

```java
OrderCreatedEvent event =
    OrderCreatedEvent.newBuilder()
        .setOrderId("ORD-10010")
        .setOrderStatus("CREATED")
        .setOrderTotal(49.9)
        .setQuantity(22)
        .build();
```

If a field is allowed to be null, the Avro schema must explicitly define a nullable union, for example:

```json
{
  "name": "message",
  "type": ["null", "string"],
  "default": null
}
```

Do not make required fields nullable just to hide invalid data.

### Problem 7: Kafka GetOffsetShell command

An earlier attempt to use:

```text
kafka.tools.GetOffsetShell
```

failed because the command was not available in that form in the Docker environment.

Working command:

```powershell
docker exec broker kafka-get-offsets --bootstrap-server broker:29092 --topic order-created-topic
```

## 13. Verification and Testing

### Verify Kafka topic

```powershell
docker exec broker kafka-topics --bootstrap-server broker:29092 --describe --topic order-created-topic
```

### Verify messages are being appended

```powershell
docker exec broker kafka-get-offsets --bootstrap-server broker:29092 --topic order-created-topic
```

### Verify application logs

For Order Service, check that the event is published.

For Inventory Service, check that the listener receives the event and executes business logic.

For Order Service again, check that the inventory response event is received.

A successful end-to-end test should show:

```text
Order Service:
Order created and event published

Inventory Service:
Order event received
Inventory checked
Inventory updated
Inventory response published

Order Service:
Inventory response received
Order status updated
```

## 14. Important Learnings

1. Kafka producer and consumer responsibilities are independent.
2. A microservice can be both a producer and a consumer.
3. `KafkaTemplate.send()` publishes a message.
4. `@KafkaListener` receives a message.
5. Avro provides a structured format for event data.
6. Schema Registry manages registered schemas.
7. Avro Maven plugin generates Java classes from schema files.
8. Producer and consumer serializers/deserializers must match the message format.
9. Consumer group IDs determine how consumers share work.
10. Different event types should represent different business occurrences.
11. An event-driven response is asynchronous, so the original request does not wait for the response in the same way as an HTTP call.
12. Correlation IDs such as `orderId` help connect related events.

## 15. Future Improvements

- Implement actual inventory database updates.
- Handle insufficient stock using an inventory rejection event.
- Add error handling and retry mechanisms.
- Make consumers idempotent to handle duplicate messages.
- Use correlation IDs to track related events.
- Move common Avro schemas into a shared Maven module.
- Add proper event versioning and schema compatibility checks.
- Implement reliable database-to-Kafka publishing using the transactional outbox pattern.
- Add integration tests for both services.

## 16. Conclusion

This project demonstrates how two Spring Boot microservices can communicate asynchronously using Apache Kafka and Avro.

The Order Service publishes an order event, the Inventory Service consumes and processes it, and then the Inventory Service publishes a separate response event. The Order Service consumes that response and updates the order status.

The project provides a foundation for understanding event-driven microservice architecture, message serialization, schema management, and asynchronous communication.
