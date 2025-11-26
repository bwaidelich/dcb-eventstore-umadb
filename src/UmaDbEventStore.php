<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreUmaDb;

use DateTimeImmutable;
use Psr\Clock\ClockInterface;
use RuntimeException;
use UmaDB\AppendCondition as UmaDBAppendCondition;
use UmaDB\Client as UmaDBClient;
use UmaDB\Event as UmaDBEvent;
use UmaDB\Exception\IntegrityException as UmaDBIntegrityException;
use UmaDB\Query as UmaDBQuery;
use UmaDB\QueryItem as UmaDBQueryItem;
use Wwwision\DCBEventStore\AppendCondition\AppendCondition;
use Wwwision\DCBEventStore\Event\Event;
use Wwwision\DCBEventStore\Event\Events;
use Wwwision\DCBEventStore\EventStore;
use Wwwision\DCBEventStore\Exceptions\ConditionalAppendFailed;
use Wwwision\DCBEventStore\Query\Query;
use Wwwision\DCBEventStore\ReadOptions;
use Wwwision\DCBEventStore\SequencedEvents;

final readonly class UmaDbEventStore implements EventStore
{
    private ClockInterface $clock;
    private UmaDBClient $client;

    public function __construct(
        string $hostname,
        ClockInterface|null $clock = null,
    ) {
        $this->client = new UmaDBClient($hostname);
        $this->clock = $clock ?? new class implements ClockInterface {
            public function now(): DateTimeImmutable
            {
                return new DateTimeImmutable();
            }
        };
    }

    public function read(Query $query, ?ReadOptions $options = null): SequencedEvents
    {
        $convertedQuery = $this->convertQuery($query);
        $call = $this->client->read(
            query: $convertedQuery,
            start: $options->from->value ?? null,
            backwards: $options->backwards ?? null,
            limit: $options->limit ?? null,
        );
        return new UmaDbEventStream($call, $this->clock);
    }

    public function append(Event|Events $events, ?AppendCondition $condition = null): void
    {
        if ($events instanceof Event) {
            $events = Events::fromArray([$events]);
        }
        $convertedEvents = [];
        foreach ($events as $event) {
            $convertedEvents[] = new UmaDBEvent(
                event_type: $event->type->value,
                data: $event->data->value,
                tags: $event->tags->toStrings(),
            );
        }

        $convertedCondition = null;
        if ($condition !== null) {
            $convertedCondition = new UmaDBAppendCondition(
                fail_if_events_match: $this->convertQuery($condition->failIfEventsMatch),
                after: $condition->after->value ?? null,
            );
        }
        try {
            $this->client->append($convertedEvents, $convertedCondition);
        } catch (UmaDBIntegrityException) {
            if ($condition !== null) {
                $condition->after !== null ? throw ConditionalAppendFailed::becauseMatchingEventsExistAfterSequencePosition($condition->after) : throw ConditionalAppendFailed::becauseMatchingEventsExist();
            }
        } catch (\Throwable $e) {
            throw new RuntimeException(sprintf('Exception while appending events: %s', $e->getMessage()), 1764013361, $e);
        }
    }

    private function convertQuery(Query $query): UmaDBQuery
    {
        $queryItems = [];
        foreach ($query as $item) {
            $queryItems[] = new UmaDBQueryItem(
                types: $item->eventTypes?->toStringArray() ?? [],
                tags: $item->tags?->toStrings() ?? []
            );
        }
        return new UmaDBQuery($queryItems);
    }
}
