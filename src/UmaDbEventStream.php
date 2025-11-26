<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreUmaDb;

use Psr\Clock\ClockInterface;
use Traversable;
use UmaDB\Client as UmaDBClient;
use UmaDB\Event as UmaDBEvent;
use UmaDB\SequencedEvent as UmaDBSequencedEvent;
use UmaDB\Query as UmaDBQuery;
use UmaDB\QueryItem as UmaDBQueryItem;
use UmaDB\AppendCondition as UmaDBAppendCondition;
use Wwwision\DCBEventStore\Event\Event;
use Wwwision\DCBEventStore\Event\SequencePosition;
use Wwwision\DCBEventStore\Event\Tags;
use Wwwision\DCBEventStore\SequencedEvent\SequencedEvent;
use Wwwision\DCBEventStore\SequencedEvents;

final readonly class UmaDbEventStream implements SequencedEvents
{
    /**
     * @param array<UmaDBSequencedEvent> $sequencedEvents
     * @param ClockInterface $clock
     */
    public function __construct(
        private array $sequencedEvents,
        private ClockInterface $clock,
    ) {
    }

    public function getIterator(): Traversable
    {
        foreach ($this->sequencedEvents as $sequencedEvent) {
            yield $this->convertEvent($sequencedEvent);
        }
    }

    public function first(): ?SequencedEvent
    {
        foreach ($this->getIterator() as $event) {
            return $event;
        }
        return null;
    }

    // -----------------------------------

    private function convertEvent(UmaDBSequencedEvent $sequencedEvent): SequencedEvent
    {
        $umaDbEvent = $sequencedEvent->getEvent();
        return new SequencedEvent(
            SequencePosition::fromInteger((int)$sequencedEvent->getPosition()),
            $this->clock->now(),
            Event::create(
                type: $umaDbEvent->getEventType(),
                data: $umaDbEvent->getData(),
                tags: Tags::fromArray(iterator_to_array($umaDbEvent->getTags())),
            ),
        );
    }
}
