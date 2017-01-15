#!/usr/bin/env python
import asyncio
import asyncio.streams

import logging
import json
from datetime import datetime

import argparse
import asyncio_redis

'''
MSG FORMAT: <msg_len><splitter>msg<splitter>
Simple interaction: Client -> Server -> All Clients
'''

MSG_MSX_LEN = 1000
SPLITTER = '\r\n'
FORMAT = '%(asctime)-15s : %(name)s : %(levelname)s : %(message)s'


class Server(object):
    OK_MSG = ('OK%s' % SPLITTER).encode('utf-8')
    ERROR_MSG = ('ERROR%s' % SPLITTER).encode('utf-8')
    MIN_LEN = 5

    def __init__(self, ip, port, redis_ip='127.0.0.1', redis_port=6379):
        self.server = None
        self.ip = ip
        self.port = port
        self.clients = {}
        self.connection = None

        self.redis_ip = redis_ip
        self.redis_port = redis_port

        self.logger = logging.getLogger('tcp-server')

    async def init_db(self):
        self.connection = await asyncio_redis.Connection.create(host=self.redis_ip,
                                                                port=self.redis_port)

    def _accept_client(self, client_reader, client_writer):
        task = asyncio.Task(self._handle_client(client_reader, client_writer))
        self.clients[task] = (client_reader, client_writer)

        def client_done(task):
            self.logger.info('Client task done')
            del self.clients[task]

        task.add_done_callback(client_done)

    async def _handle_client(self, client_reader, client_writer):
        await self.init_db()
        buffer = bytearray()
        while True:
            data = await client_reader.readline()
            if not data:
                break

            self.logger.info('Get from client: %s' % data.decode('utf-8'))
            buffer.extend(data)

            if len(buffer) > self.MIN_LEN:
                str_data = buffer.decode()
                self.logger.info('Start processing: %s' % str_data)

                l, msg, _ = str_data.split('\r\n')
                msg_len = len(msg)

                if int(l) == msg_len:
                    json_msg = json.loads(msg)
                    if self.connection:
                        self.logger.info('Saving history')
                        # TODO: append
                        await self.connection.set(
                            json_msg['author'],
                            '{date}:{message}'.format(
                                date=datetime.now(),
                                message=json_msg['msg']
                            )
                        )
                    else:
                        self.logger.info('No connection. Skip')

                    client_writer.write(self.OK_MSG)
                    await client_writer.drain()
                else:
                    self.logger.info('Still cannot process')

    def start(self, loop):
        server_stream = asyncio.streams.start_server(self._accept_client,
                                                     host=self.ip, port=self.port,
                                                     loop=loop)
        loop.run_until_complete(server_stream)

    def stop(self, loop):
        if self.server is not None:
            self.server.close()
            loop.run_until_complete(self.server.wait_closed())
            self.server = None


class Client(object):

    def __init__(self, loop, ip, port):
        self.loop = loop
        self.ip = ip
        self.port = port
        self.logger = logging.getLogger('tcp-client')

    @asyncio.coroutine
    def run(self, author, init_msg):
        reader, writer = yield from asyncio.streams.open_connection(
            self.ip, self.port, loop=self.loop)

        def send(msg):
            encoded_msg = ('{length}{split}{msg}{split}'.format(length=len(msg),
                                                                split=SPLITTER,
                                                                msg=msg)).encode('utf-8')
            self.logger.info('Send: %s' % str(encoded_msg))
            writer.write(encoded_msg)

        def recv():
            decoded_msg = (yield from reader.readline()).decode('utf-8').strip()
            self.logger.info('Got: %s' % decoded_msg)
            return decoded_msg

        send(json.dumps({'author': author,
                         'msg': init_msg}))
        while True:
            msg = yield from recv()
            if msg == 'OK':
                self.logger.info('Successfully sent')
                break
            # TODO: Error type/msg
            elif msg == 'ERROR':
                self.logger.info('Some error: %s' % msg)
                break

        writer.close()
        yield from asyncio.sleep(0.5)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--type', type=str, required=True,
                        help='server|client', choices=['server', 'client'])
    parser.add_argument('-s', '--server_ip', type=str, required=True,
                        help='server IP')
    parser.add_argument('-p', '--server_port', type=str, required=True,
                        help='server port')
    parser.add_argument('-l', '--logging_level', type=str, required=True,
                        help='info|debug|error', choices=['info', 'debug', 'error'])
    args = parser.parse_args()
    loop = asyncio.get_event_loop()

    log_level = getattr(logging, args.logging_level.upper())
    logging.basicConfig(level=log_level, format=FORMAT)
    logger = logging.getLogger('base')

    if args.type == 'server':
        logger.info('Running server...')
        server = Server(ip=args.server_ip, port=args.server_port)
        server.start(loop)

        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass
    elif args.type == 'client':
        logger.info('Running client...')
        client = Client(loop=loop, ip=args.server_ip, port=args.server_port)
        try:
            # TODO: get author/message
            loop.run_until_complete(client.run('mike111', 'Cool message'))
        finally:
            loop.close()
    else:
        logger.error('Nothing to do. Run a server or a client')

if __name__ == '__main__':
    main()
