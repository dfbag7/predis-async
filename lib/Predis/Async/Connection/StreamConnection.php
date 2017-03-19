<?php

/*
 * This file is part of the Predis\Async package.
 *
 * (c) Daniele Alessandri <suppakilla@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Predis\Async\Connection;

use Predis\Command\CommandInterface;
use Predis\Connection\ConnectionParametersInterface;
use Predis\ResponseError;

use InvalidArgumentException;
use SplQueue;
use Predis\ResponseQueued;
use Predis\Async\Buffer\StringBuffer;

use Clue\Redis\Protocol\Model\StatusReply;
use Clue\Redis\Protocol\Model\ErrorReply;
use Clue\Redis\Protocol\Parser\ResponseParser;
use Clue\Redis\Protocol\Serializer\RecursiveSerializer;

use React\EventLoop\LoopInterface;

class StreamConnection extends AbstractConnection
{
    protected $parser = null;
    protected $serializer = null;

    /**
     * @param LoopInterface                 $loop Event loop interface.
     * @param ConnectionParametersInterface $parameters Initialization parameters for the connection.
     */
    public function __construct(LoopInterface $loop, ConnectionParametersInterface $parameters)
    {
        parent::__construct($loop, $parameters);

        $this->initializeResponseParser();
        $this->initializeRequestSerializer();
    }

    /**
     * Initializes the response parser instance.
     */
    protected function initializeResponseParser()
    {
        $this->parser = new ResponseParser();
    }

    /**
     * Initializes the request serializer instance.
     */
    protected function initializeRequestSerializer()
    {
        $this->serializer = new RecursiveSerializer();
    }

    /**
     * Parses the incoming buffer and emits objects when the buffer
     * contains one or more response payloads available for consumption.
     *
     * @param string $buffer Buffer read from the network stream.
     *
     * @return mixed
     */
    public function parseResponseBuffer($buffer)
    {
        foreach($this->parser->pushIncoming($buffer) as $response)
        {
            $value = $response->getValueNative();

            if($response instanceof StatusReply)
            {
                switch($value)
                {
                    case 'OK':
                        $response = true;
                        break;

                    case 'QUEUED':
                        $response = new ResponseQueued();
                        break;

                    default:
                        $response = $value;
                }
            }
            elseif($response  instanceof ErrorReply)
            {
                $response = new ResponseError($value);
            }
            else
            {
                $response = $value;
            }

            $this->state->process($response);
        }
    }

    /**
     * Executes a command on Redis and calls the provided callback when the
     * response has been read from the server.
     *
     * @param CommandInterface $command  Redis command.
     * @param mixed            $callback Callable object.
     */
    public function executeCommand(CommandInterface $command, $callback)
    {
        if($this->buffer->isEmpty() && $stream = $this->getResource())
        {
            $this->loop->addWriteStream($stream, $this->writableCallback);
        }

        $request = $this->serializer->getRequestMessage($command->getId(), $command->getArguments());

        $this->buffer->append($request);
        $this->commands->enqueue([$command, $callback]);
    }
}
