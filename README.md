# Multi-Region Property Listing Backend with NGINX, PostgreSQL, and Kafka

## 1. Project Overview

This project implements a distributed property listing backend that simulates two geographic regions:

- US Region
- EU Region

The system uses:

- FastAPI for backend API development
- PostgreSQL for regional databases
- Apache Kafka for asynchronous cross-region replication
- Zookeeper as a Kafka dependency
- NGINX as a reverse proxy with regional routing and failover
- Docker and Docker Compose for containerization
- Pytest for integration testing

The system supports:

- Regional request routing
- Automatic failover
- Asynchronous data replication
- Optimistic locking
- Idempotent PUT requests
- Replication lag monitoring
- Automated database seeding
- Integration testing

---

# 2. System Architecture

The architecture contains the following services:

1. nginx
2. zookeeper
3. kafka
4. db-us
5. db-eu
6. backend-us
7. backend-eu

Architecture:

    Client
       |
       v
    NGINX
    Port 8080
       |
       +--------------------+
       |                    |
       v                    v
    /us/*                 /eu/*
       |                    |
       v                    v
    backend-us          backend-eu
       |                    |
       v                    v
    db-us                db-eu
       ^                    ^
       |                    |
       +------ Kafka -------+
           property-updates

Normal routing:

    /us/* -> backend-us -> db-us

    /eu/* -> backend-eu -> db-eu

Failover routing:

    backend-us DOWN
        |
        v
    /us/* -> backend-eu

    backend-eu DOWN
        |
        v
    /eu/* -> backend-us

Replication:

    Update in US
        |
        v
    db-us
        |
        v
    Kafka property-updates
        |
        v
    backend-eu consumer
        |
        v
    db-eu

The same process happens in the opposite direction for EU updates.

---

# 3. Recommended Technology Stack

Backend:

    Python
    FastAPI

Database:

    PostgreSQL 14

ORM / Database Access:

    SQLAlchemy

Kafka:

    Apache Kafka
    kafka-python or confluent-kafka

Reverse Proxy:

    NGINX

Containerization:

    Docker
    Docker Compose

Testing:

    Pytest
    requests

---

# 4. Recommended Project Structure

    multi-region-property-backend/
    |
    |-- docker-compose.yml
    |-- .env
    |-- .env.example
    |-- README.md
    |
    |-- nginx/
    |   |
    |   `-- nginx.conf
    |
    |-- backend/
    |   |
    |   |-- Dockerfile
    |   |-- requirements.txt
    |   |
    |   `-- app/
    |       |
    |       |-- __init__.py
    |       |-- main.py
    |       |-- config.py
    |       |-- database.py
    |       |-- models.py
    |       |-- schemas.py
    |       |-- routes.py
    |       |-- kafka_producer.py
    |       |-- kafka_consumer.py
    |       `-- idempotency.py
    |
    |-- seeds/
    |   |
    |   |-- init.sql
    |   |-- seed.py
    |   `-- housing.csv
    |
    `-- tests/
        |
        |-- test_concurrency.py
        |-- test_replication.py
        |-- test_idempotency.py
        `-- demonstrate_failover.sh

The same backend image can be used for both backend-us and backend-eu.

The behavior of each backend is controlled using the REGION environment variable.

Example:

    backend-us:
        REGION=us

    backend-eu:
        REGION=eu

---

# 5. Step 1 - Create the Project Structure

Create the project folders:

    mkdir multi-region-property-backend

    cd multi-region-property-backend

Create:

    backend/
    nginx/
    seeds/
    tests/

Create the required files:

    docker-compose.yml
    .env
    .env.example
    README.md

---

# 6. Step 2 - Configure Environment Variables

Create a .env file for local development.

Example:

    POSTGRES_USER=postgres
    POSTGRES_PASSWORD=postgres
    POSTGRES_DB=properties

    KAFKA_BROKER=kafka:29092

The .env file should not be committed if it contains real secrets.

Create .env.example:

    POSTGRES_USER=your_postgres_user
    POSTGRES_PASSWORD=your_postgres_password
    POSTGRES_DB=properties
    KAFKA_BROKER=kafka:29092

The .env.example file should be committed to Git.

---

# 7. Step 3 - Create the Database Schema

Both db-us and db-eu must contain a properties table.

Required schema:

    CREATE TABLE properties (
        id BIGINT PRIMARY KEY,
        price DECIMAL NOT NULL,
        bedrooms INTEGER NOT NULL,
        bathrooms INTEGER NOT NULL,
        region_origin VARCHAR(2) NOT NULL,
        version INTEGER NOT NULL DEFAULT 1,
        updated_at TIMESTAMP NOT NULL DEFAULT NOW()
    );

The table contains:

    id
        Unique property ID.

    price
        Property price.

    bedrooms
        Number of bedrooms.

    bathrooms
        Number of bathrooms.

    region_origin
        Region that originated the latest update.
        Values should be "us" or "eu".

    version
        Version used for optimistic locking.

    updated_at
        Timestamp of the latest update.

---

# 8. Step 4 - Create an Idempotency Table

Create another table:

    CREATE TABLE processed_requests (
        request_id VARCHAR(255) PRIMARY KEY,
        created_at TIMESTAMP NOT NULL DEFAULT NOW()
    );

This table stores processed X-Request-ID values.

When a PUT request arrives:

    1. Read X-Request-ID.
    2. Check processed_requests.
    3. If it already exists, return HTTP 422.
    4. Otherwise process the update.
    5. Store the request ID.

For better consistency, the request-ID insertion and property update should be performed in the same database transaction where possible.

---

# 9. Step 5 - Prepare the Housing Dataset

Choose a public housing/property dataset.

The dataset must contain enough rows to create at least 1000 properties.

Required normalized fields:

    id
    price
    bedrooms
    bathrooms

Additional columns can be ignored.

Create a Python seed script:

    seeds/seed.py

The script should:

    1. Read the CSV dataset.
    2. Normalize required columns.
    3. Generate IDs if necessary.
    4. Assign region_origin.
    5. Set version = 1.
    6. Insert at least 1000 records.

Both db-us and db-eu must contain the complete dataset.

Example:

    Property 1
    id = 1
    price = 250000
    bedrooms = 3
    bathrooms = 2
    region_origin = us
    version = 1

You can divide the dataset logically:

    First half:
        region_origin = us

    Second half:
        region_origin = eu

Both databases should still receive all rows.

---

# 10. Step 6 - Automate Database Seeding

The databases must initialize automatically.

PostgreSQL supports:

    /docker-entrypoint-initdb.d/

Mount initialization scripts into this directory.

Example Docker Compose volume:

    volumes:
      - ./seeds/init.sql:/docker-entrypoint-initdb.d/01-init.sql

If CSV processing requires Python, you can create a dedicated seed mechanism or generate SQL inserts before startup.

Verification:

    docker compose up -d

Connect to db-us.

Verify:

    SELECT COUNT(*) FROM properties;

Expected:

    count >= 1000

Repeat for db-eu.

---

# 11. Step 7 - Build the FastAPI Backend

Create:

    backend/app/main.py

The backend should expose:

    GET /health

    GET /properties/{id}

    PUT /properties/{id}

    GET /replication-lag

The same application runs in both regions.

Use:

    REGION=us

or:

    REGION=eu

to identify the current region.

---

# 12. Step 8 - Implement Health Endpoint

Create:

    GET /health

Response:

    {
        "status": "healthy",
        "region": "us"
    }

EU response:

    {
        "status": "healthy",
        "region": "eu"
    }

This helps verify routing and failover.

---

# 13. Step 9 - Configure Database Connections

backend-us connects to:

    db-us

backend-eu connects to:

    db-eu

Example:

    backend-us:
        DATABASE_URL=postgresql://user:password@db-us:5432/properties

    backend-eu:
        DATABASE_URL=postgresql://user:password@db-eu:5432/properties

Do not hardcode credentials in application source code.

Read them from environment variables.

---

# 14. Step 10 - Implement GET Property Endpoint

Endpoint:

    GET /properties/{id}

Example:

    GET /properties/1

Response:

    {
        "id": 1,
        "price": 250000,
        "bedrooms": 3,
        "bathrooms": 2,
        "region_origin": "us",
        "version": 1,
        "updated_at": "2026-07-17T10:00:00Z"
    }

If the property does not exist:

    HTTP 404 Not Found

---

# 15. Step 11 - Implement Property Update Endpoint

Endpoint:

    PUT /properties/{id}

Request header:

    X-Request-ID: unique-uuid

Request:

    {
        "price": 500000,
        "version": 1
    }

Successful response:

    HTTP 200 OK

    {
        "id": 1,
        "price": 500000,
        "version": 2,
        "updated_at": "2026-07-17T10:30:00Z"
    }

The backend must:

    1. Validate X-Request-ID.
    2. Check for duplicate request ID.
    3. Validate property version.
    4. Update local PostgreSQL database.
    5. Increment version.
    6. Update updated_at.
    7. Set region_origin to the current region.
    8. Publish Kafka event.
    9. Return updated property.

---

# 16. Step 12 - Implement Optimistic Locking

Optimistic locking prevents two users from overwriting each other's updates.

Suppose property 1 currently has:

    version = 1

Request A sends:

    {
        "price": 500000,
        "version": 1
    }

Request B also sends:

    {
        "price": 550000,
        "version": 1
    }

Only one should succeed.

Use an atomic SQL update:

    UPDATE properties
    SET
        price = :price,
        version = version + 1,
        region_origin = :region,
        updated_at = NOW()
    WHERE
        id = :id
        AND version = :version;

Check affected rows.

If:

    affected_rows = 1

The update succeeded.

If:

    affected_rows = 0

Return:

    HTTP 409 Conflict

Example response:

    {
        "detail": "Version conflict. Re-fetch the latest property and retry."
    }

Do not implement automatic conflict merging.

The README should explain that the client should:

    1. Receive 409.
    2. Fetch the latest property.
    3. Review the new version.
    4. Retry using the latest version.

---

# 17. Step 13 - Implement Idempotency

Every PUT request must contain:

    X-Request-ID

Example:

    X-Request-ID: 550e8400-e29b-41d4-a716-446655440000

Before processing:

    SELECT request_id
    FROM processed_requests
    WHERE request_id = ?;

If found:

    HTTP 422 Unprocessable Entity

Response:

    {
        "detail": "Duplicate request"
    }

If not found:

    Process the request.

Then store:

    INSERT INTO processed_requests(request_id)
    VALUES (?);

The second request with the same X-Request-ID must not update the property again.

---

# 18. Step 14 - Configure Kafka

Create Kafka topic:

    property-updates

Both backends connect to:

    kafka:29092

The backend requires:

    KAFKA_BROKER=kafka:29092

Kafka should start after Zookeeper is available.

The backend should wait until Kafka and PostgreSQL are ready before becoming healthy.

---

# 19. Step 15 - Implement Kafka Producer

After a successful PUT request, publish an event.

Topic:

    property-updates

Message:

    {
        "id": 123,
        "price": 500000.00,
        "bedrooms": 3,
        "bathrooms": 2,
        "region_origin": "us",
        "version": 2,
        "updated_at": "2026-07-17T10:30:00Z"
    }

Important:

Only publish after the local database update succeeds.

Do not publish events for:

    409 Conflict

    422 Duplicate Request

    404 Property Not Found

---

# 20. Step 16 - Implement Kafka Consumer

Each backend runs a Kafka consumer.

backend-us:

    Listens to property-updates.

    Ignore:
        region_origin = us

    Process:
        region_origin = eu

backend-eu:

    Listens to property-updates.

    Ignore:
        region_origin = eu

    Process:
        region_origin = us

Example:

    US property updated
        |
        v
    Kafka event
        |
        v
    backend-eu consumes event
        |
        v
    db-eu updated

---

# 21. Step 17 - Prevent Replication Loops

Kafka consumer updates must NOT produce another Kafka message.

Otherwise:

    US update
        |
        v
    Kafka
        |
        v
    EU consumer
        |
        v
    Kafka
        |
        v
    US consumer
        |
        v
    Infinite loop

Therefore:

    API update -> publish Kafka event

    Kafka consumer update -> DO NOT publish Kafka event

---

# 22. Step 18 - Handle Replication Versions

When consuming an event, compare versions.

Example:

    Incoming version = 5

    Local version = 4

Apply the event.

If:

    Incoming version <= Local version

Ignore the event.

This prevents an older Kafka event from overwriting newer data.

A possible query:

    UPDATE properties
    SET
        price = :price,
        bedrooms = :bedrooms,
        bathrooms = :bathrooms,
        region_origin = :region_origin,
        version = :version,
        updated_at = :updated_at
    WHERE
        id = :id
        AND version < :version;

This makes replication safer against duplicate or out-of-order messages.

---

# 23. Step 19 - Implement Replication Lag Endpoint

Endpoint:

    GET /replication-lag

Required response:

    {
        "lag_seconds": 2.5
    }

Track the timestamp of the last successfully consumed Kafka message.

Calculate:

    current_time - last_consumed_message_updated_at

Return the difference in seconds.

Example:

    {
        "lag_seconds": 2.5
    }

For persistence and multi-process safety, consider storing the latest consumed timestamp in PostgreSQL instead of only keeping it in Python memory.

---

# 24. Step 20 - Dockerize the Backend

Create:

    backend/Dockerfile

Example structure:

    FROM python:3.12-slim

    WORKDIR /app

    COPY requirements.txt .

    RUN pip install --no-cache-dir -r requirements.txt

    COPY . .

    CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

requirements.txt should include packages such as:

    fastapi
    uvicorn
    sqlalchemy
    psycopg2-binary
    kafka-python
    pydantic
    pytest
    requests

---

# 25. Step 21 - Configure Docker Compose

docker-compose.yml must define:

    nginx
    zookeeper
    kafka
    db-us
    db-eu
    backend-us
    backend-eu

Every service must include a healthcheck.

Database healthcheck:

    pg_isready

Backend healthcheck:

    curl http://localhost:8000/health

Kafka healthcheck:

    Check Kafka broker availability.

NGINX healthcheck:

    Request a valid local endpoint.

All services should become healthy within 3 minutes.

---

# 26. Step 22 - Configure NGINX Regional Routing

NGINX listens on:

    localhost:8080

Routing:

    /us/* -> backend-us

    /eu/* -> backend-eu

Important:

If NGINX strips the /us/ or /eu/ prefix before forwarding, the FastAPI application can expose:

    /health

    /properties/{id}

    /replication-lag

For example:

    Client:
        /us/health

    NGINX forwards:
        /health

    backend-us receives:
        /health

---

# 27. Step 23 - Configure NGINX Failover

US routing:

    Primary:
        backend-us

    Backup:
        backend-eu

EU routing:

    Primary:
        backend-eu

    Backup:
        backend-us

Configure retry behavior for:

    connection error
    timeout
    HTTP 500
    HTTP 502
    HTTP 503
    HTTP 504

Important:

The health_check directive shown in the task example is generally an NGINX Plus feature.

If using the standard open-source nginx Docker image, configure passive failover using upstream servers, backup servers, max_fails, fail_timeout, and proxy_next_upstream.

Do not rely on the health_check directive unless your NGINX distribution supports it.

---

# 28. Step 24 - Configure NGINX Logging

Create a custom log format.

It must include:

    $upstream_response_time

Example output:

    upstream_response_time=0.005

Verify using:

    docker logs nginx_proxy

or by checking the configured NGINX access log.

---

# 29. Step 25 - Test Regional Routing

Start the project:

    docker compose up -d --build

Check US:

    curl http://localhost:8080/us/health

Expected:

    {
        "status": "healthy",
        "region": "us"
    }

Check EU:

    curl http://localhost:8080/eu/health

Expected:

    {
        "status": "healthy",
        "region": "eu"
    }

Check container logs to confirm correct routing.

---

# 30. Step 26 - Test Property Update

First determine the current property version.

Then send:

    curl -X PUT http://localhost:8080/us/properties/1 \
      -H "Content-Type: application/json" \
      -H "X-Request-ID: unique-request-1" \
      -d '{"price":500000,"version":1}'

Expected:

    HTTP 200

Response:

    {
        "id": 1,
        "price": 500000,
        "version": 2,
        "updated_at": "..."
    }

Verify db-us.

---

# 31. Step 27 - Test Optimistic Locking

First request:

    version = 1

Expected:

    HTTP 200

Property version becomes:

    version = 2

Send another request using:

    version = 1

Expected:

    HTTP 409 Conflict

The outdated update must not modify the database.

---

# 32. Step 28 - Test Idempotency

Generate a request ID.

Example:

    test-request-123

Send:

    PUT /us/properties/4

Header:

    X-Request-ID: test-request-123

Expected:

    HTTP 200

Send exactly the same request again.

Expected:

    HTTP 422

Response:

    {
        "detail": "Duplicate request"
    }

The property must not be updated twice.

---

# 33. Step 29 - Test Kafka Publication

Start a Kafka console consumer for:

    property-updates

Send a successful PUT request.

Verify that Kafka receives:

    {
        "id": 1,
        "price": 500000,
        "bedrooms": 3,
        "bathrooms": 2,
        "region_origin": "us",
        "version": 2,
        "updated_at": "..."
    }

---

# 34. Step 30 - Test Cross-Region Replication

Update:

    /us/properties/2

Wait approximately 5 seconds.

Query db-eu.

The EU database should contain:

    Same price
    Same version
    Same updated_at
    region_origin = us

Then test the opposite direction.

Update:

    /eu/properties/3

Verify db-us receives the replicated update.

---

# 35. Step 31 - Test Replication Lag

Update a property in US.

Wait 2 to 3 seconds.

Request:

    GET /eu/replication-lag

Expected:

    {
        "lag_seconds": 2.5
    }

The exact number may vary.

It should be a valid non-negative number after the consumer has processed a message.

---

# 36. Step 32 - Create Concurrent Update Test

Create:

    tests/test_concurrency.py

The test should:

    1. Select one property.
    2. Determine its current version.
    3. Send concurrent updates.
    4. Use the same version for both updates.
    5. Use different X-Request-ID values.
    6. Verify conflict handling.

Expected result:

    One request succeeds.

    One request fails with 409.

For testing optimistic locking within one database, both concurrent requests should target the same regional endpoint.

For a more advanced cross-region test, document that asynchronous multi-primary replication introduces additional distributed conflict considerations.

The required optimistic-locking contract is satisfied by preventing concurrent stale-version writes at the database level.

---

# 37. Step 33 - Create Failover Demonstration Script

Create:

    tests/demonstrate_failover.sh

The script should:

    1. Start Docker Compose.
    2. Wait until services are healthy.
    3. Request /us/health.
    4. Confirm backend-us handled it.
    5. Stop backend-us.
    6. Request /us/health again.
    7. Verify HTTP 200.
    8. Verify backend-eu handled the failover request.
    9. Restart backend-us if desired.

Example flow:

    docker compose up -d

    curl http://localhost:8080/us/health

    docker stop backend-us

    curl http://localhost:8080/us/health

Expected:

    First response:
        region = us

    After failure:
        region = eu

This demonstrates NGINX failover.

---

# 38. Step 34 - Add Health Checks

Every Docker Compose service must have a healthcheck.

Required:

    nginx
    zookeeper
    kafka
    db-us
    db-eu
    backend-us
    backend-eu

Use depends_on with service health conditions where supported.

Startup order should approximately be:

    PostgreSQL
        |
        v
    Kafka / Zookeeper
        |
        v
    Backend services
        |
        v
    NGINX

The entire system should become healthy within 3 minutes.

---

# 39. Step 35 - Recommended Development Order

Implement the project in this exact order:

    1. Create folder structure.

    2. Create .env and .env.example.

    3. Create PostgreSQL containers.

    4. Create properties table.

    5. Create processed_requests table.

    6. Seed both databases with 1000+ rows.

    7. Build FastAPI application.

    8. Connect backend-us to db-us.

    9. Connect backend-eu to db-eu.

    10. Implement /health.

    11. Implement GET /properties/{id}.

    12. Implement PUT /properties/{id}.

    13. Implement optimistic locking.

    14. Implement X-Request-ID idempotency.

    15. Configure Kafka.

    16. Create property-updates topic.

    17. Implement Kafka producer.

    18. Implement Kafka consumer.

    19. Implement cross-region replication.

    20. Prevent Kafka replication loops.

    21. Handle duplicate/out-of-order Kafka events.

    22. Implement /replication-lag.

    23. Configure NGINX regional routing.

    24. Configure NGINX failover.

    25. Configure NGINX custom logging.

    26. Add Docker healthchecks.

    27. Write concurrency integration test.

    28. Write replication tests.

    29. Write idempotency tests.

    30. Create demonstrate_failover.sh.

    31. Test complete system.

    32. Write README.md.

    33. Push project to GitHub.

---

# 40. Final Verification Checklist

## Docker

Run:

    docker compose up -d --build

Verify all seven services are running and healthy.

Required:

    nginx
    zookeeper
    kafka
    db-us
    db-eu
    backend-us
    backend-eu

---

## Database

Verify both databases contain:

    properties table

    processed_requests table

Verify:

    SELECT COUNT(*) FROM properties;

Expected:

    At least 1000 rows.

---

## Routing

Verify:

    GET /us/health -> backend-us

    GET /eu/health -> backend-eu

---

## Failover

Stop:

    backend-us

Verify:

    GET /us/health

still returns:

    HTTP 200

and is handled by:

    backend-eu

---

## Property Update

Verify:

    PUT /us/properties/1

returns:

    HTTP 200

and increments version.

---

## Optimistic Locking

Verify stale version returns:

    HTTP 409

---

## Idempotency

Verify duplicate X-Request-ID returns:

    HTTP 422

---

## Kafka

Verify successful update publishes to:

    property-updates

---

## Replication

Verify:

    US update -> db-eu

and:

    EU update -> db-us

---

## Replication Lag

Verify:

    GET /eu/replication-lag

returns:

    {
        "lag_seconds": number
    }

---

## NGINX Logs

Verify logs contain:

    upstream_response_time

---

## Tests

Verify tests directory contains:

    test_concurrency.py

    demonstrate_failover.sh

Additional recommended tests:

    test_replication.py

    test_idempotency.py

---

# 41. Submission Checklist

The Git repository should contain:

    docker-compose.yml

    .env.example

    README.md

    nginx/nginx.conf

    backend/Dockerfile

    backend/requirements.txt

    backend/app/

    seeds/

    tests/

Do not commit:

    .env

    __pycache__/

    .pytest_cache/

    virtual environment folders

    database credentials

---

# 42. Important Design Decisions to Explain in README

## Optimistic Locking

Every property contains a version number.

Updates use an atomic conditional query.

If the requested version does not match the current version:

    HTTP 409 Conflict

The client should fetch the latest record and retry.

---

## Idempotency

Each PUT request requires:

    X-Request-ID

Previously processed request IDs are stored in PostgreSQL.

Duplicate IDs return:

    HTTP 422

This prevents accidental duplicate updates caused by retries.

---

## Kafka Replication

Regional updates are first committed locally.

After success, an event is published to:

    property-updates

The opposite region consumes the event and updates its database.

This provides asynchronous eventual consistency.

---

## Replication Loop Prevention

Each event contains:

    region_origin

A backend ignores events originating from its own region.

Consumer-applied database updates do not produce new Kafka events.

---

## Out-of-Order Event Protection

Kafka consumer updates are applied only when:

    incoming_version > local_version

Older or duplicate events are ignored.

---

## NGINX Failover

NGINX provides:

    Regional routing

    /us/* -> backend-us

    /eu/* -> backend-eu

It also provides fallback routing when the primary backend is unavailable.

---

# 43. Final Architecture Flow

Normal US update:

    Client
        |
        v
    NGINX
        |
        v
    backend-us
        |
        v
    Optimistic Lock Check
        |
        v
    Idempotency Check
        |
        v
    db-us Update
        |
        v
    Kafka Producer
        |
        v
    property-updates
        |
        v
    backend-eu Consumer
        |
        v
    Version Check
        |
        v
    db-eu Update

Failover:

    Client
        |
        v
    /us/*
        |
        v
    NGINX
        |
        v
    backend-us unavailable
        |
        v
    backend-eu
        |
        v
    Request handled successfully

Optimistic locking:

    Request A -> version 1 -> SUCCESS -> version 2

    Request B -> version 1 -> 409 CONFLICT

Idempotency:

    Request ID ABC -> SUCCESS

    Request ID ABC again -> 422 DUPLICATE REQUEST

The completed project should start using a single command:

    docker compose up -d --build

After startup, the application should be accessible through:

    http://localhost:8080
