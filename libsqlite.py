#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# libsqlite.py
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
import hashlib
import logging
import os
import random
import time
from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import Generator, Optional

import aiosqlite
import base62

logger = logging.getLogger("url-shorter").getChild("libsqlite")
logger.setLevel(logging.DEBUG)


_DROP_STATEMENT = '''
    DROP TABLE IF EXISTS "mapper";
    DROP TABLE IF EXISTS "users";
'''


_CREATE_STATEMENT = '''
    CREATE TABLE "mapper" (
        "short_name"	TEXT NOT NULL,
        "from_user"	    INTEGER NOT NULL,
        "target_uri"	TEXT NOT NULL UNIQUE,
        "create_date"	INTEGER NOT NULL,
        "last_query"    INTEGER NOT NULL,
        PRIMARY KEY("short_name")
    );

    CREATE TABLE "users" (
        "id"        INTEGER NOT NUll,
        "string"    TEXT NOT NULL UNIQUE,
        PRIMARY KEY("string")
    );
'''


class SqliteBase(metaclass=ABCMeta):
    def __init__(self, file_name: str):
        self.file_name = file_name
        self.lock = asyncio.Lock()

    @classmethod
    async def _new(cls, file_name: str, drop_statement: str, create_statement: str, *,
                   main_table_name: str, renew: bool = False) -> SqliteBase:
        if renew:
            try:
                os.remove(file_name)
            except FileNotFoundError:
                pass
        async with aiosqlite.connect(file_name) as db:
            async with db.execute('''SELECT name FROM sqlite_master WHERE type = 'table' AND name = ? ''',
                                  (main_table_name,)) as cursor:
                if (await cursor.fetchone()) is not None:
                    logger.debug('Found database, load it')
                    return cls(file_name)
            logger.debug('Create new database structure')
            async with db.executescript(drop_statement):
                pass
            async with db.executescript(create_statement):
                pass
        return cls(file_name)

    @classmethod
    @abstractmethod
    async def new(cls, file_name: str, *, renew: bool = False) -> SqliteBase:
        return NotImplemented


class UrlDatabase(SqliteBase):

    class StatusCode(Enum):
        NotOwner = "NotOwner"
        NotFound = "NotFound"

    @classmethod
    async def new(cls, file_name: str, *, renew: bool = False) -> UrlDatabase:
        return await cls._new(file_name, _DROP_STATEMENT, _CREATE_STATEMENT, main_table_name="mapper", renew=renew)

    async def get_all_authorized_key(self) -> Generator[str, None, None]:
        async with self.lock, aiosqlite.connect(self.file_name) as db:
            async with db.execute('''SELECT "string" FROM "users"''') as cursor:
                for user_row in await cursor.fetchall():
                    yield user_row[0]

    async def insert_authorized_key(self, user_id: int) -> str:
        async with self.lock, aiosqlite.connect(self.file_name) as db:
            s = hashlib.sha256(f'{user_id}{random.random()}'.encode()).hexdigest()
            async with db.execute('''INSERT INTO "users" VALUES (?, ?)''', (user_id, s)):
                pass
            await db.commit()
            return s

    async def delete_user(self, user_id: int) -> None:
        async with self.lock, aiosqlite.connect(self.file_name) as db:
            async with db.execute('''DELETE FROM "users" WHERE "id" = ?''', (user_id,)):
                pass

    async def insert_uri(self, original_uri: str, from_user: int) -> str:
        async with self.lock, aiosqlite.connect(self.file_name) as db:
            async with db.execute('''SELECT "short_name" FROM "mapper" WHERE "target_uri" = ?''',
                                  (original_uri, )) as cursor:
                if r := await cursor.fetchone():
                    return r[0]
            hash_value = hashlib.sha256(f'{original_uri}{random.random()}').digest()
            r = base62.encodebytes(hash_value)[:10]
            async with db.execute('''INSERT INTO "mapper" VALUES (?, ?, ?, ?)''',
                                  (r, from_user, original_uri, int(time.time()))):
                pass
            return r

    async def delete_uri(self, target_uri: str, from_user: int) -> Optional[StatusCode]:
        async with self.lock, aiosqlite.connect(self.file_name) as db:
            async with db.execute('''SELECT "from_user" FROM "mapper" WHERE "target_uri" = ?''',
                                  (target_uri,)) as cursor:
                if not (r := await cursor.fetchone):
                    return UrlDatabase.StatusCode.NotFound
                if r[0] != from_user:
                    return UrlDatabase.StatusCode.NotOwner
                async with db.execute('''DELETE FROM "mapper" WHERE "target_uri" = ?''',
                                      (target_uri,)):
                    pass


