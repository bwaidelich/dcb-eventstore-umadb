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
use UmaDB\SequencedEvent as UmaDBSequencedEvent;
use Webmozart\Assert\Assert;
use Wwwision\DCBEventStore\AppendCondition\AppendCondition;
use Wwwision\DCBEventStore\Event\Event;
use Wwwision\DCBEventStore\Event\Events;
use Wwwision\DCBEventStore\Event\Tags;
use Wwwision\DCBEventStore\EventStore;
use Wwwision\DCBEventStore\Exceptions\ConditionalAppendFailed;
use Wwwision\DCBEventStore\Query\Query;
use Wwwision\DCBEventStore\ReadOptions;
use Wwwision\DCBEventStore\SequencedEvent\SequencedEvent;
use Wwwision\DCBEventStore\SequencedEvent\SequencedEvents;
use Wwwision\DCBEventStore\SequencedEvent\SequencePosition;

final readonly class UmaDbEventStore implements EventStore
{

    public function __construct(
        private UmaDBClient $client,
        private ClockInterface $clock,
    ) {
    }

    public static function create(
        string $url,
        string|null $caPath = null,
        int|null $batchSize = null,
        ClockInterface|null $clock = null,
    ): self {
        if ($clock === null) {
            $clock = new class implements ClockInterface {
                public function now(): DateTimeImmutable
                {
                    return new DateTimeImmutable();
                }
            };
        }
        $client = new UmaDBClient($url, $caPath, $batchSize);
        return new self($client, $clock);
    }

    public function read(Query $query, ?ReadOptions $options = null): SequencedEvents
    {
        $convertedQuery = $this->convertQuery($query);
        $umaSequencedEvents = $this->client->read(
            query: $convertedQuery,
            start: $options->from->value ?? null,
            backwards: $options->backwards ?? null,
            limit: $options->limit ?? null,
        );
        return SequencedEvents::create(static function () use ($umaSequencedEvents) {
            foreach ($umaSequencedEvents as $umaSequencedEvent) {
                yield self::convertEvent($umaSequencedEvent);
            }
        });
    }

    public function append(Event|Events $events, ?AppendCondition $condition = null): void
    {
        if ($events instanceof Event) {
            $events = Events::fromArray([$events]);
        }
        $convertedEvents = [];
        foreach ($events as $event) {
            $data = [
                'payload' => $event->data->value,
                'metadata' => $event->metadata->value,
                'recordedAt' => $this->clock->now()->format(DATE_ATOM),
            ];
            $convertedEvents[] = new UmaDBEvent(
                event_type: $event->type->value,
                data: json_encode($data, JSON_THROW_ON_ERROR),
                tags: $event->tags->toStrings(),
                uuid: isset($event->metadata->value['id']) && is_string($event->metadata->value['id']) ? $event->metadata->value['id'] : null
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

    private static function convertEvent(UmaDBSequencedEvent $sequencedEvent): SequencedEvent
    {
        $umaDbEvent = $sequencedEvent->getEvent();
        $data = json_decode($umaDbEvent->getData(), true, 512, JSON_THROW_ON_ERROR);
        Assert::isArray($data);
        Assert::string($data['recordedAt']);
        Assert::string($data['payload']);
        Assert::isMap($data['metadata']);
        $recordedAt = DateTimeImmutable::createFromFormat(DATE_ATOM, $data['recordedAt']);
        Assert::isInstanceOf($recordedAt, DateTimeImmutable::class);
        return new SequencedEvent(
            SequencePosition::fromInteger($sequencedEvent->getPosition()),
            $recordedAt,
            Event::create(
                type: $umaDbEvent->getEventType(),
                data: $data['payload'],
                tags: Tags::fromArray(iterator_to_array($umaDbEvent->getTags())),
                metadata: $data['metadata'],
            ),
        );
    }
}
