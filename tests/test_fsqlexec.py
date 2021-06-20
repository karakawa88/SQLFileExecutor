import sys
import os
sys.path.append(os.path.abspath("./src"))
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
from fsqlexec import SQLFileExecutor
from fsqlexec import check_file_list_exists, fname_line_to_array, create_sql_files
from fsqlexec import cmd
from click.testing import CliRunner
from logging import getLogger
import sys

logger = getLogger(__name__)

class FSQLExecTest(unittest.TestCase):
    """fsqlexecモジュールのテスト。コマンドもテストする。
    """
    TEST_TABLES: list[str] = ["test", "blog_entry"]
    TEST_INDEX: list[str] = ["test_id_index", "blog_entry_user_id_index"]
    
#     def __init__(self) -> None:
#         self.__dbcon = None

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

    def test_fname_line_to_array_ok(self) -> None:
        """fname_line_to_array()関数のテスト。
        行をリストにして返すかテストする。
        """
        fname = "tests/data/exclude_file.txt"
        result = ["tests/data/drop_all.sql", "tests/data/error_table.sql"]
        expected = fname_line_to_array(fname)
        self.assertEqual(result, expected)

    def test_fname_line_to_array_exception(self) -> None:
        """fname_line_to_array()関数のテスト。
        引数のファイル名のファイルが存在しない場合例外をスローするかテスト。
        """
        fname = "tests/data/exclude.txt"
        with self.assertRaises(IOError):
            expected = fname_line_to_array(fname)
    
    def test_create_sql_files_ok(self) -> None:
        """create_sql_files()関数のテスト。
        ファイルのリストからexclude_fileのファイルのリストを除外しているかテスト。
        """
        includes_files = [
                "tests/data/CTblog_entry.sql", "tests/data/CTtest.sql",
                "tests/data/drop_all.sql", "tests/data/error_table.sql"
        ]
        exclude_file = "tests/data/exclude_file.txt"
        result = ["tests/data/CTblog_entry.sql", "tests/data/CTtest.sql"]
        expected = create_sql_files(includes_files, exclude_file)
        self.assertEqual(result, expected)
    
    def test_cmd_normal(self) ->None:
        """コマンドを実行しSQLが実行されているかテストする。
        """
        sql_files = [
                "tests/data/CTblog_entry.sql", "tests/data/CTtest.sql",
        ]
        db_ini_file = "tests/conf/postgres.ini"
        opts = [ "--ini-file", db_ini_file ]
        opts.extend(sql_files)
        try:
            runner = CliRunner()
            result = runner.invoke(cmd, opts)
            
            expected = ["test", "blog_entry"]
            tables =  self.table_name_list()
            self.assertTrue(self.in_any(tables, expected))
        except Exception as ex:
            logger.error(str(ex))
        finally:
            self.drop_db_objects()

    def test_cmd_error_exec(self) -> None:
        """オプション--error-execでエラーが起きても処理が継続されるかテスト
        """
        sql_files = [
                        "tests/data/CTtest.sql", 
                        "tests/data/error_table.sql",
                        "tests/data/CTblog_entry.sql",
                    ]
        expected = ["test", "blog_entry"]
        db_ini_file = "tests/conf/postgres.ini"
        opts = [ "--ini-file", db_ini_file , "--error-exec" ]
        opts.extend(sql_files)
        try:
            runner = CliRunner()
            result = runner.invoke(cmd, opts)

            tables = self.table_name_list()
            self.assertTrue(match_list_fn(tables, expected))
        except Exception as ex:
            logger.error(ex)
        finally:
            self.drop_db_objects()

    def in_any(self, array: Sequence[Any], search: Sequence[Any]) -> bool:
        """配列に検索する配列の要素が含まれているか
        """
        for entry in search:
            if  entry not in array:
                return False
        return True

    def table_name_list(self) -> Sequence[str]:
        """DBのテーブル名のリストを返す。
        Returns:
            Sequence[str]: テーブル名のリスト
        Raises:
            SQLException: DB接続またはSQLエラー
        """
        db_conn = None
        cur = None
        try:
            db_conn = self.__dbcon
            cur = db_conn.cursor()
            ret = pypostgres.db_object_names("TABLE", cur)
            return ret
        except Exception as ex:
            logger.error(ex)
            raise ex
        finally:
            if db_conn is not None:
                if cur is not None:
                    db_conn.commit()
                    cur.close()

    def drop_db_objects(self) -> None:
        """テストで作成したDBのオブジェクトを全て削除する。
        Raises:
            SQLException: DB削除エラー
        """
        dbcon = None
        try:
            sql_files = ["tests/data/drop_all.sql"]
            dbcon = self.__dbcon
            sqlexec = SQLFileExecutor(sql_files, dbcon)
            sqlexec.exec()
        except Exception as ex:
            print(ex)
        finally:
            if dbcon is not None:
                dbcon.commit()
        
    def setUp(self) -> None:
        """テストの前処理
        DBの接続を取得する。
        """
        try:
            ini_file = "tests/conf/postgres.ini"
            self.__dbcon = pypostgres.get_config_connection(ini_file, "PostgreSQL")
            self.drop_db_objects()
        except Exception as ex:
            print("DB接続エラー")
            raise ex
        print(sys.path)

    def tearDown(self) -> None:
        """テストの後処理
        作成したDBのオブジェクトを削除しDBを切断する。
        """
        dbcon = self.__dbcon
        if self.__dbcon is not None:
#             self.drop_db_objects()
            dbcon.commit()
            dbcon.close()

