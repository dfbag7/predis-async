<?php

/*
 * This file is part of the Predis\Async package.
 *
 * (c) Daniele Alessandri <suppakilla@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

require __DIR__.'/../autoload.php';

$client = new Predis\Async\Client('tcp://127.0.0.1:6379');

class A
{
    public $pubsub;

    protected $i = 0;

    protected $client;

    public function __construct($client)
    {
        $this->client = $client;
    }

    public function onMessage($event, $pubsub)
    {
        $message = "Received message `%s` from channel `%s` [type: %s].\n";

        $feedback = sprintf($message,
            $event->payload,
            $event->channel,
            $event->kind
        );

        echo $feedback;

        if ($event->payload === 'quit') {
            $pubsub->quit();
        }
    }

    public function checkMemory($timer)
    {
        echo 'memory: ' .  memory_get_usage() . PHP_EOL;

        if( (++$this->i) >= 10)
        {
            $this->pubsub->quit();
            $this->client->getEventLoop()->stop();
        }
    }
}

$a = new A($client);

$client->connect(function ($client) use($a) {
    echo "Connected to Redis, now listening for incoming messages...\n";

    $a->pubsub = $client->pubsub('nrk:channel', [$a, 'onMessage']);
});

$loop = $client->getEventLoop();

$loop->addPeriodicTimer(5, [$a, 'checkMemory']);

$loop->run();
