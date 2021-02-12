#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# unit_test.py
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
import unittest
import asyncio
import subprocess

import aiohttp
import aiofiles


def run_in_coroutine(f):
    task = asyncio.get_event_loop().run_until_complete(f)
    return task


class ServerHandler:
    process: subprocess.Popen = None
    authorized_key = None
    location = None

    @staticmethod
    def get_header() -> dict[str, str]:
        d = {'X-Real-IP': '127.0.0.1'}
        if ServerHandler.authorized_key is not None:
            d.update(dict(authorization=f'Bearer 1:{ServerHandler.authorized_key}'))
        return d


class TestServer(unittest.TestCase):

    def test_0_homepage(self):
        async def _homepage():
            async with aiohttp.ClientSession(raise_for_status=True) as session:
                async with session.get('http://127.0.0.1:31625/') as req, aiofiles.open('data/index.html') as fin:
                    return await asyncio.gather(req.read(), fin.read())
        htm, f = run_in_coroutine(_homepage())
        self.assertEqual(htm.decode(), f)

    def test_1_add_user(self):
        async def _add_user():
            async with aiohttp.ClientSession(headers=ServerHandler.get_header(),
                                             raise_for_status=True) as session:
                async with session.post('http://127.0.0.1:31625/user', data=dict(id=1, super_user=True)) as req:
                    ServerHandler.authorized_key = (await req.json()).get('result')
            return ServerHandler.authorized_key
        self.assertIsNotNone(run_in_coroutine(_add_user()))

    def test_2_add_new_url(self):
        async def _add_new_url():
            async with aiohttp.ClientSession(headers=ServerHandler.get_header(),
                                             raise_for_status=True) as session:
                async with session.post('http://127.0.0.1:31625/', data=dict(url='https://google.com/')) as req:
                    ServerHandler.location = (await req.json()).get('location')
                return ServerHandler.location
        self.assertIsNotNone(ServerHandler.get_header().get('authorization'))
        self.assertIsNotNone(run_in_coroutine(_add_new_url()))

    def test_3_redirect(self):
        async def _test_redirect():
            async with aiohttp.ClientSession(raise_for_status=True) as session:
                async with session.get(f'http://127.0.0.1:31625/{ServerHandler.location}', allow_redirects=True) as req:
                    return req.host
        self.assertIsNotNone(ServerHandler.get_header().get('authorization'))
        self.assertEqual(run_in_coroutine(_test_redirect()), 'www.google.com')

    def test_4_delete_user(self):
        async def _delete_user():
            async with aiohttp.ClientSession(headers=ServerHandler.get_header(),
                                             raise_for_status=True) as session:
                async with session.delete('http://127.0.0.1:31625/user', data=dict(id=1)) as req:
                    return await req.json()
        self.assertDictEqual(run_in_coroutine(_delete_user()), dict(status=200, body='ok'))


if __name__ == '__main__':
    unittest.main(failfast=True)
