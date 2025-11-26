<?php

/**
 * UmaDB PHP Extension Stubs
 *
 * These stubs provide IDE autocompletion and type hints for the UmaDB extension.
 * This file is for development only and should not be included in production.
 *
 * @see https://github.com/bwaidelich/umadb-php
 */

namespace UmaDB {

    /**
     * UmaDB client for connecting to and interacting with an UmaDB server
     */
    class Client
    {
        /**
         * Create a new UmaDB client and connect to the server
         *
         * @param string $url Server URL (e.g., "http://localhost:50051")
         * @param string|null $ca_path Optional path to CA certificate file for TLS
         * @param int|null $batch_size Optional batch size for reading events
         * @throws \UmaDB\Exception\TransportException If connection fails
         */
        public function __construct(string $url, ?string $ca_path = null, ?int $batch_size = null) {}

        /**
         * Read events from the event store
         *
         * @param Query|null $query Optional query to filter events
         * @param int|null $start Optional starting position
         * @param bool|null $backwards Read backwards from start position
         * @param int|null $limit Optional maximum number of events to return
         * @param bool|null $subscribe Subscribe to new events (streaming)
         * @return SequencedEvent[] Array of sequenced events
         * @throws \UmaDB\Exception\UmaDBException On error
         */
        public function read(
            ?Query $query = null,
            ?int $start = null,
            ?bool $backwards = null,
            ?int $limit = null,
            ?bool $subscribe = null
        ): array {}

        /**
         * Get the current head position of the event store
         *
         * @return int|null Position of the last event, or null if store is empty
         * @throws \UmaDB\Exception\UmaDBException On error
         */
        public function head(): ?int {}

        /**
         * Append events to the event store
         *
         * @param Event[] $events Array of events to append
         * @param AppendCondition|null $condition Optional condition for optimistic concurrency control
         * @return int Position of the last appended event
         * @throws \UmaDB\Exception\IntegrityException If append condition fails
         * @throws \UmaDB\Exception\UmaDBException On other errors
         */
        public function append(array $events, ?AppendCondition $condition = null): int {}
    }

    /**
     * Represents an event in the UmaDB event store
     */
    class Event
    {
        /**
         * Create a new Event
         *
         * @param string $event_type The event type identifier
         * @param string $data Binary event data
         * @param string[]|null $tags Optional array of tags for filtering
         * @param string|null $uuid Optional UUID string for idempotency
         */
        public function __construct(
            string $event_type,
            string $data,
            ?array $tags = null,
            ?string $uuid = null
        ) {}

        /**
         * Get the event type
         *
         * @return string
         */
        public function getEventType(): string {}

        /**
         * Get the event data as a string
         *
         * @return string
         */
        public function getData(): string {}

        /**
         * Get the tags
         *
         * @return string[]
         */
        public function getTags(): array {}

        /**
         * Get the UUID
         *
         * @return string|null
         */
        public function getUuid(): ?string {}

        /**
         * String representation
         *
         * @return string
         */
        public function __toString(): string {}
    }

    /**
     * Represents an event with its position in the event stream
     */
    class SequencedEvent
    {
        /**
         * Get the event
         *
         * @return Event
         */
        public function getEvent(): Event {}

        /**
         * Get the position in the stream
         *
         * @return int
         */
        public function getPosition(): int {}

        /**
         * String representation
         *
         * @return string
         */
        public function __toString(): string {}
    }

    /**
     * A query item specifying event types and tags to match
     *
     * An event matches a query item if:
     * - Its type is in the item's types (or item has no types)
     * - All item tags are present in the event's tags
     */
    class QueryItem
    {
        /**
         * Create a new QueryItem
         *
         * @param string[]|null $types Optional array of event types to match
         * @param string[]|null $tags Optional array of tags (all must be present)
         */
        public function __construct(?array $types = null, ?array $tags = null) {}

        /**
         * Get the types
         *
         * @return string[]
         */
        public function getTypes(): array {}

        /**
         * Get the tags
         *
         * @return string[]
         */
        public function getTags(): array {}
    }

    /**
     * A query for filtering events
     *
     * An event matches the query if it matches any of the query items (OR logic)
     */
    class Query
    {
        /**
         * Create a new Query
         *
         * @param QueryItem[]|null $items Optional array of query items
         */
        public function __construct(?array $items = null) {}

        /**
         * Get the query items
         *
         * @return QueryItem[]
         */
        public function getItems(): array {}
    }

    /**
     * Condition for conditional appends
     *
     * Allows enforcing business rules and optimistic concurrency control
     */
    class AppendCondition
    {
        /**
         * Create a new AppendCondition
         *
         * @param Query $fail_if_events_match Query that must not match any existing events
         * @param int|null $after Optional position constraint (fail if events exist after this position)
         */
        public function __construct(Query $fail_if_events_match, ?int $after = null) {}

        /**
         * Get the query that must not match
         *
         * @return Query
         */
        public function getFailIfEventsMatch(): Query {}

        /**
         * Get the position constraint
         *
         * @return int|null
         */
        public function getAfter(): ?int {}
    }
}

namespace UmaDB\Exception {

    /**
     * Base exception for all UmaDB errors
     */
    class UmaDBException extends \Exception {}

    /**
     * Thrown when append conditions fail or integrity constraints are violated
     */
    class IntegrityException extends UmaDBException {}

    /**
     * Thrown when gRPC transport/communication fails
     */
    class TransportException extends UmaDBException {}

    /**
     * Thrown when data corruption is detected
     */
    class CorruptionException extends UmaDBException {}

    /**
     * Thrown when file system I/O operations fail
     */
    class IoException extends UmaDBException {}
}
