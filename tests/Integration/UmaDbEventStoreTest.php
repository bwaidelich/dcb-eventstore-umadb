<?php
declare(strict_types=1);

namespace Wwwision\DCBEventStoreUmaDb\Tests\Integration;

use Closure;
use DateTimeImmutable;
use PHPUnit\Framework\Attributes\CoversClass;
use Psr\Clock\ClockInterface;
use Testcontainers\Container\GenericContainer;
use Testcontainers\Container\StartedTestContainer;
use Testcontainers\Wait\WaitForLog;
use Wwwision\DCBEventStore\Query\Query;
use Wwwision\DCBEventStore\Tests\Integration\EventStoreTestBase;
use Wwwision\DCBEventStoreUmaDb\UmaDbEventStore;

#[CoversClass(UmaDbEventStore::class)]
final class UmaDbEventStoreTest extends EventStoreTestBase
{
    private StartedTestContainer|null $testContainer = null;

    public function createEventStore(): UmaDbEventStore
    {
        if ($this->testContainer === null) {
            $this->testContainer = new GenericContainer('umadb/umadb')
                ->withExposedPorts(50051)
                ->withWait(new WaitForLog('UmaDB started'))
                ->start();
        }
        return UmaDbEventStore::create('http://' . $this->testContainer->getHost() . ':' . $this->testContainer->getMappedPort(50051), clock: $this->getTestClock());
    }

    public function tearDown(): void
    {
        $this->testContainer?->stop();
        $this->testContainer = null;
    }

}