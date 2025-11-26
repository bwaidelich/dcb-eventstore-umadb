<?php
declare(strict_types=1);

namespace Wwwision\DCBEventStoreUmaDb\Tests\Integration;

use PHPUnit\Framework\Attributes\CoversNothing;
use RuntimeException;
use Testcontainers\Container\GenericContainer;
use Testcontainers\Container\StartedTestContainer;
use Testcontainers\Wait\WaitForLog;
use Wwwision\DCBEventStore\EventStore;
use Wwwision\DCBEventStore\Query\Query;
use Wwwision\DCBEventStore\Tests\Integration\EventStoreConcurrencyTestBase;
use Wwwision\DCBEventStoreUmaDb\UmaDbEventStore;

require_once __DIR__ . '/../../vendor/autoload.php';

#[CoversNothing]
final class ConcurrencyTest extends EventStoreConcurrencyTestBase
{

    private static UmaDbEventStore|null $eventStore = null;
    private static StartedTestContainer|null $testContainer = null;

    public static function prepare(): void
    {
        $eventStore = self::createEventStore();
        if ($eventStore->read(Query::all())->first() !== null) {
            throw new RuntimeException('The event store must not contain any events before preforming consistency tests');
        }
    }

    public static function cleanup(): void
    {
        self::$testContainer?->stop();
        self::$testContainer = null;
        putenv('DCB_TEST_UMADB_PORT');
    }

    protected static function createEventStore(): EventStore
    {
        if (self::$eventStore === null) {
            $mappedPort = getenv('DCB_TEST_UMADB_PORT');
            if (!is_string($mappedPort)) {
                self::$testContainer = new GenericContainer('umadb-umadb')
                    ->withExposedPorts(50051)
                    ->withWait(new WaitForLog('UmaDB server'))
                    ->start();
                $mappedPort = self::$testContainer->getMappedPort(50051);
                putenv('DCB_TEST_UMADB_PORT=' . $mappedPort);
            }
            self::$eventStore = new UmaDbEventStore('http://127.0.0.1:' . $mappedPort);
        }
        return self::$eventStore;
    }

}