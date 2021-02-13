#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# shorter.py
# Copyright (C) 2021 KunoiSayami
#
# This module is part of url-shorter and is released under
# the AGPL v3 License: https://www.gnu.org/licenses/agpl-3.0.txt
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
from __future__ import annotations
import asyncio
import logging
import signal
import sys
from configparser import ConfigParser
from typing import Union

import aioredis
import aiofiles
# import toml
from aiohttp import web

from libsqlite import UrlDatabase


logger = logging.getLogger("url-shorter").getChild('server')
logger.setLevel(logging.DEBUG)


class Server:
    paused = False

    def __init__(self, redis_connection: aioredis.Redis, url_database: UrlDatabase, bind_address: str,
                 bind_port: int, *, debug: bool = False) -> None:
        self.redis = redis_connection
        self.url_conn = url_database
        self.bind_address = bind_address
        self.bind_port = bind_port
        self.website = web.Application()
        self.runner = web.AppRunner(self.website)
        self.site = None
        self.debug_mode = debug

    @classmethod
    async def new(cls, redis_address: str, database_filename: str, post_database_statement: str,
                  bind_address: str, bind_port: int, *, debug: bool = False) -> Server:
        redis_connection, conn = await asyncio.gather(
            aioredis.create_redis_pool(redis_address),
            UrlDatabase.new(database_filename, post_database_statement, renew=debug)
        )
        await redis_connection.delete('us_auth')
        keys = [key async for key in conn.get_all_authorized_key()]
        if keys:
            await redis_connection.sadd('us_auth', *keys)
        return cls(redis_connection, conn, bind_address, bind_port, debug=debug)

    def init_router(self) -> None:
        self.website.add_routes([
            web.post('/user', self.handle_add_user),
            web.delete('/user', self.handle_delete_user),
            web.get('/', self.handle_help_page),
            web.delete('/{url}', self.handle_delete_link),
            web.put('/', self.handle_revoke_key),
            web.get('/{url}', self.handle_redirect),
            web.post('/', self.handle_create_link),
        ])

    async def save_query_result(self, url: str, location: str) -> None:
        await self.redis.set(f'us_{url}', location)
        if location != 'Null':
            # TODO: log function
            pass

    async def handle_delete_user(self, request: web.Request) -> web.Response:
        if request.headers.get('X-Real-IP', '0.0.0.0') != '127.0.0.1':
            return web.HTTPForbidden()
        if user := int((await request.post()).get('id', '0')):
            await self.url_conn.delete_user(user)
            return web.json_response(dict(status=200, body='ok'))
        return web.HTTPBadRequest()

    async def handle_add_user(self, request: web.Request) -> web.Response:
        if request.headers.get('X-Real-IP', '0.0.0.0') != '127.0.0.1':
            return web.HTTPForbidden()
        if user := int((field := await request.post()).get('id', '0')):
            r = await self.url_conn.insert_authorized_key(
                user,
                super_user=bool(field.get('super_user', 'false').lower())
            )
            await self.redis.sadd('us_auth', r)
            return web.json_response(dict(status=200, result=r))
        if self.debug_mode:
            logger.debug('Post user => %d', user)
        return web.HTTPBadRequest()

    async def handle_redirect(self, request: web.Request) -> web.Response:
        if not (url := request.match_info.get('url')):
            return await self.handle_help_page(request)
        if redis_result := await self.redis.get(f'us_{url}'):
            url = redis_result.decode()
            if url != 'Null':
                return web.HTTPMovedPermanently(url)
            return web.HTTPBadRequest()
        if result := await self.url_conn.query_url(url):
            asyncio.run_coroutine_threadsafe(self.save_query_result(url, result), asyncio.get_event_loop())
            return web.HTTPMovedPermanently(url)
        asyncio.run_coroutine_threadsafe(self.save_query_result(url, "Null"), asyncio.get_event_loop())
        return web.HTTPBadRequest()

    async def handle_create_link(self, request: web.Request) -> web.Response:
        # Authorization: Bearer user_id:auth_string
        if not isinstance(bearer := await self.verify_identify(request), str):
            return bearer
        data = await request.post()
        if not (url := data.get('url')):
            return self.log_and_return(
                web.HTTPBadRequest(reason='Post format unexpected'),
                'Deny bad format %s %s',
                request)
        result = await self.url_conn.insert_url(url, int(bearer.split(':')[0]))
        return web.json_response({'location': result})

    @staticmethod
    def log_and_return(return_value: web.Response, log_body: str, request: web.Request, *args) -> web.Response:
        logger.error(log_body,
                     request.headers.get('X-Real-IP', request.remote),
                     request.path,
                     *args)
        return return_value

    async def verify_identify(self, request: web.Request) -> Union[str, web.Response]:
        if not (bearer := request.headers.get('authorization')):
            return self.log_and_return(web.HTTPForbidden(), 'Deny unauthorized request %s %s', request)
        if not await self.redis.sismember('us_auth', (bearer := bearer.split(maxsplit=1)[1]).split(':')[1]):
            return self.log_and_return(web.HTTPForbidden(), 'Deny unauthorized key %s %s', request)
        return bearer

    async def handle_delete_link(self, request: web.Request) -> web.Response:
        if not isinstance(bearer := await self.verify_identify(request), str):
            return bearer
        data = await request.post()
        if not (url := data.get('url')):
            return self.log_and_return(
                web.HTTPBadRequest(reason='Post format unexpected'),
                'Deny bad format %s %s',
                request)
        if rt := await self.url_conn.delete_url(url, int(bearer.split(':')[0])):
            if rt == UrlDatabase.StatusCode.NotOwner:
                return web.json_response(dict(text='Url is not your own created', code=1), status=400)
            elif rt == UrlDatabase.StatusCode.NotFound:
                return web.json_response(dict(text='Url not found', code=2), status=404)
            return web.HTTPBadRequest()
        return web.HTTPNoContent(content_type='text/html')

    async def handle_revoke_key(self, request: web.Request) -> web.Response:
        if not isinstance(bearer := await self.verify_identify(request), str):
            return bearer
        new_key = await self.url_conn.update_authorized_key(int(bearer.split(':')[0]))
        return web.json_response(dict(key=new_key))

    @staticmethod
    async def handle_help_page(_request: web.Request) -> web.Response:
        try:
            async with aiofiles.open('data/index.html') as fin:
                return web.Response(body=await fin.read())
        except FileNotFoundError:
            raise web.HTTPNotFound()

    async def start(self) -> None:
        self.init_router()
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, self.bind_address, self.bind_port)
        await self.site.start()
        logger.info('Server is listening on %s:%d', self.bind_address, self.bind_port)

    async def stop(self) -> None:
        Server.paused = False
        self.redis.close()
        await self.site.stop()
        await self.runner.cleanup()
        await self.redis.wait_closed()

    @classmethod
    async def load_from_configure_file(cls, file_name: str = 'data/config.ini', *, debug: bool = False) -> Server:
        config = ConfigParser()
        config.read(file_name)
        return await cls.new(
            config.get('redis', 'address', fallback='redis://localhost'),
            config.get('database', 'filename', fallback='data/us.db'),
            config.get('database', 'post_statement', fallback=''),
            config.get('server', 'bind'),
            config.getint('server', 'port'),
            debug=debug
            )

    @staticmethod
    async def idle() -> None:
        def _bind_SIGINT(*_args) -> None:
            Server.paused = False
        Server.paused = True

        for sig in (signal.SIGINT, signal.SIGABRT, signal.SIGTERM):
            signal.signal(sig, _bind_SIGINT)

        while Server.paused:
            await asyncio.sleep(0.5)


async def main(debug: bool = False):
    server = await Server.load_from_configure_file(debug=debug)
    await server.start()
    await server.idle()
    await server.stop()


if __name__ == '__main__':
    try:
        import coloredlogs

        coloredlogs.install(
            logging.DEBUG,
            fmt='%(asctime)s,%(msecs)03d - %(levelname)s - %(module)s - %(funcName)s - %(lineno)d - %(message)s'
        )
    except ModuleNotFoundError:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(lineno)d - %(message)s'
        )
    asyncio.get_event_loop().run_until_complete(main('--debug' in sys.argv))
