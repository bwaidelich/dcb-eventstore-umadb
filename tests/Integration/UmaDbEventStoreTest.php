<?php
declare(strict_types=1);

namespace Wwwision\DCBEventStoreUmaDb\Tests\Integration;

use PHPUnit\Framework\Attributes\CoversClass;
use Testcontainers\Container\GenericContainer;
use Testcontainers\Container\StartedTestContainer;
use Testcontainers\Wait\WaitForLog;
use Wwwision\DCBEventStore\Tests\Integration\EventStoreTestBase;
use Wwwision\DCBEventStoreUmaDb\UmaDbEventStore;

#[CoversClass(UmaDbEventStore::class)]
final class UmaDbEventStoreTest extends EventStoreTestBase
{
    private StartedTestContainer|null $testContainer = null;

    protected function createEventStore(): UmaDbEventStore
    {
        if ($this->testContainer === null) {
            $this->testContainer = new GenericContainer('umadb-umadb')
                ->withExposedPorts(50051)
                ->withWait(new WaitForLog('UmaDB server'))
                ->start();
        }
        return new UmaDbEventStore('http://127.0.0.1:' . $this->testContainer->getMappedPort(50051));
    }

    public function tearDown(): void
    {
        $this->testContainer?->stop();
        $this->testContainer = null;
    }

}