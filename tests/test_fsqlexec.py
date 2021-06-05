from typing import Optional, Union, Any
from typing import NoReturn
from typing import Dict
from typing import TypeVar, Generic, NewType, Type, ClassVar
from typing import IO, TextIO, BinaryIO
from collections.abc import Callable
from collections.abc import Sequence, Iterable

import psycopg2
import unittest
from db import pypostgres
from db.SQLException import SQLException
from SQLFileExecutor import SQLFileExecutor
from SQLFileExecutor.fsqlexec import check_file_list_exists, fname_line_to_array, create_SQLFileExecutor

class FSQLExecTest(unittest.TestCase):
    """fsqlexecモジュールのテスト。コマンドもテストする。
    """
    def test_check_file_list_exists_ok(self) -> None:
        """ファイルのリストのファイルが存在するか確認する関数のテスト。
        Raises:
            IOError: ファイルが存在しない
        """
        sql_files = ["tests/data/CTblog_entry.sql", "tests/data/CTtest.sql"]
        self.assertTrue(check_file_list_exists(sql_files))

    def test_check_file_list_exists_exception(self) -> None:
        """ファイルのリストのファイルが存在しない場合例外を投げるかテスト。
        Raises:
            IOError: ファイルが存在しない
        """
        sql_files = ["tests/data/CTblog_entry.sql", "tests/data/CTtest.sql", "tests/data/CThogehoge.sql"]
        with self.assertRaises(IOError):
            self.assertTrue(check_file_list_exists(sql_files))

# def check_file_list_exists(files: Iterable[str]) -> None:
# def fname_line_to_array(fname: Optional[str]) -> list[str]:
# def create_sql_files(include_file: Sequence[str], exclude_file: str) -> list[str]:
