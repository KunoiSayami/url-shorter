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


import aioredis
from aiohttp import web

from libsqlite import UrlDatabase


logger = logging.getLogger("url-shorter").getChild('server')
logger.setLevel(logging.DEBUG)


class Server:
    def __init__(self, redis_connection: aioredis.Redis, url_database: UrlDatabase, bind_address: str,
                 bind_port: int) -> None:
        self.redis = redis_connection
        self.url_conn = url_database
        self.bind_address = bind_address
        self.bind_port = bind_port
        self.website = web.Application()
        self.runner = web.AppRunner(self.website)
        self.site = None

    @classmethod
    async def new(cls, redis_address: str, database_filename: str, post_database_statement: str,
                  bind_address: str, bind_port: int) -> Server:
        redis_connection, conn = await asyncio.gather(aioredis.create_redis_pool(redis_address),
                                                      UrlDatabase.new(database_filename, post_database_statement))
        return cls(redis_connection, conn, bind_address, bind_port)

    def init_router(self) -> None:
        self.website.add_routes([
            web.get('/{url}', self.handle_redirect),
            web.post('/', self.handle_create_link),
            web.delete('/{url}', self.handle_delete_link),
            web.put('/', self.handle_revoke_key),
        ])

    async def save_query_result(self, url: str, location: str) -> None:
        await self.redis.set(f'us_{url}', location)
        if location != 'Null':
            # TODO: log function
            pass

    async def handle_delete_user(self, request: web.Request) -> web.Response:
        pass

    async def handle_add_user(self, request: web.Request) -> web.Response:
        pass

    async def handle_redirect(self, request: web.Request) -> web.Response:
        if not (url := request.match_info.get('name')):
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
        bearer = request.headers.get('Authorization')
        # Authorization: Bearer user_id:auth_string
        if bearer is None:
            return self.log_and_return(web.HTTPForbidden(), 'Deny unauthorized request %s %s', request)
        if not await self.redis.sismember('us_auth', bearer := bearer.split(maxsplit=1)[1]):
            return self.log_and_return(web.HTTPForbidden(), 'Deny unauthorized key %s %s', request)
        data = await request.post()
        if not (url := data.get('url')):
            return self.log_and_return(
                web.HTTPBadRequest(reason='Post format unexpected'),
                'Deny bad format %s %s',
                request)
        result = await self.url_conn.insert_url(url, int(bearer.split(':')[0]))
        return web.json_response({'location': result})

    @staticmethod
    def log_and_return(return_value: web.Response, log_body: str, request: web.Request) -> web.Response:
        logger.error(log_body,
                     request.headers.get('X-Real-IP', request.remote),
                     request.path)
        return return_value

    async def handle_delete_link(self, request: web.Request) -> web.Response:
        pass

    async def handle_revoke_key(self, request: web.Request) -> web.Response:
        pass

    async def handle_help_page(self, request: web.Request) -> web.Response:
        pass

    async def start(self) -> None:
        self.init_router()
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, self.bind_address, self.bind_port)
        await self.site.start()
        logger.info('Server is listening on %s:%d', self.bind_address, self.bind_port)

    async def stop(self) -> None:
        await self.site.stop()
        await self.runner.cleanup()


async def main():
    pass


if __name__ == '__main__':
    pass
