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
from fsqlexec  import SQLFileExecutor
from logging import getLogger
import os

logger = getLogger(__name__)

# PostgreSQLのカーソルの型
CUR = TypeVar('CUR')
class SQLFileExecutorTest(unittest.TestCase):
    TEST_TABLES: list[str] = ["test", "blog_entry"]
    TEST_INDEX: list[str] = ["test_id_index", "blog_entry_user_id_index"]
    TEST_SYSTEM_CATALOG = [
            { 
                "type": "TABLE",
                "sql": "select tablename from pg_tables"
            },
            { 
                "type": "INDEX",
                "sql": "select indexname from pg_indexes"
            }
    ]

    def db_object_names(self, type_name: str, cur: CUR) -> list[str]:
        """DBオブジェクトの名前のリストを取得して返す。
        Args:
            type_name (str): オブジェクトのタイプ
            cur (CUR):      カーソルオブジェクト
        Returns:
            list[str]: DBオブジェクトの名前のリスト
        """
        # DBのオブジェクトの名前でシステムカタログのSQLを取得する
        def find_system_catalog_sql(type_name: str) -> Optional[str]:
            for v in SQLFileExecutorTest.TEST_SYSTEM_CATALOG:
                if v["type"] == type_name:
                    return v["sql"]
            return None

        sql = None
        sql = find_system_catalog_sql(type_name)
        print("system catalog sql: " + sql)
        cur.execute(sql)
        result = cur.fetchall()
        return [row[0] for row in result]
    

    def match_list_fn(self, match_list: list[str], search_list: list[str]) -> bool:
        for v in search_list:
            if v not in match_list:
                return False
        return True

    def test_sql_exec(self) -> None:
        """ SQLFileExecutorクラスのSQL実行をテストする。
        テストSQL文ファイルで作成したテーブルインデックスが作成されたかテストする。
        Raises:
            AssertionError: テスト失敗
        """

        sql_files = ["tests/data/CTtest.sql", "tests/data/CTblog_entry.sql"]
        dbcon = self.__dbcon
        try:
            self.drop_db_objects()
            sqlexec = SQLFileExecutor(sql_files, dbcon)
            sqlexec.exec()

            cur = dbcon.cursor()
            tables = self.db_object_names("TABLE", cur)
            indexs = self.db_object_names("INDEX", cur)
            self.assertTrue(self.match_list_fn(tables, SQLFileExecutorTest.TEST_TABLES)
                                and self.match_list_fn(indexs, SQLFileExecutorTest.TEST_INDEX))
        except Exception as ex:
            raise ex

    def setUp(self) -> None:
        """テスト前の前処理。
        DB接続を行いConnectionオブジェクトを取得する。
        Raises:
            IOError: DB接続情報のiniファイル読み込みエラー
            Exception: DB接続エラー
        """
        sys.path.append(os.path.abspath("./src"))
        try:
            # DB接続
            ini_file = "conf/postgres.ini"
            self.__dbcon = pypostgres.get_config_connection(ini_file, 'PostgreSQL')
            print("DB接続", self.__dbcon)
        except IOError as ie:
            print(ie)
            raise ie
        except Exception as ex:
            print(ex)
    
    def test_exec_error(self) -> None:
        """SQL文でエラーが発生した場合はSQLExceptionをスローするかテストする。
        Reises:
            SQLException: SQLエラー
        """
        sql_files = [
                        "tests/data/CTtest.sql", 
                        "tests/data/CTblog_entry.sql",
                        "tests/data/error_table.sql"
                    ]
        dbcon = self.__dbcon
        with self.assertRaises(SQLException):
            try:
                self.drop_db_objects()
                sqlexec = SQLFileExecutor(sql_files, dbcon)
                sqlexec.exec()
            except Exception as ex:
                raise ex
            finally:
                self.drop_db_objects()
                
    def test_error_exec(self) -> None:
        """SQLFileExecutorのerror_execフラグをテストする。
        """
        sql_files = [
                        "tests/data/CTtest.sql", 
                        "tests/data/error_table.sql",
                        "tests/data/CTblog_entry.sql",
                    ]
        expected = ["test", "blog_entry"]
        dbcon = self.__dbcon
        sqlexec = None
        try:
            sqlexec = SQLFileExecutor(sql_files, dbcon, True)
            sqlexec.exec()
                
            cur = dbcon.cursor()
            tables = self.db_object_names("TABLE", cur)
            self.assertTrue(self.match_list_fn(tables, expected))
        except Exception as ex:
            logger.error(ex)
        finally:
            self.drop_db_objects()

    def test_error_exec_no(self) -> None:
        """error_execフラグがFalseのエラーが起きたら処理を終了するかテスト。
        """
        sql_files = [
                        "tests/data/CTtest.sql", 
                        "tests/data/error_table.sql",
                        "tests/data/CTblog_entry.sql",
                    ]
        expected = ["test"]
        dbcon = self.__dbcon
        try:
            sqlexec = SQLFileExecutor(sql_files, dbcon, True)
            sqlexec.exec()
        except Exception as ex:
            logger.error(ex)
            cur = dbcon.cursor()
            tables = self.db_object_names("TABLE", cur)
            self.assertTrue(self.match_list_fn(tables, expected) and len(sqlexec.errors) >= 1)


    def drop_db_objects(self) -> None:
        """テストで作成したDBのオブジェクトを全て削除する。
        Raises:
            SQLException: DB削除エラー
        """
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


    def tearDown(self) -> None:
        """テストの後処理
        作成したDBのオブジェクトを削除しDBを切断する。
        """
        dbcon = self.__dbcon
        if self.__dbcon is not None:
            self.drop_db_objects()
            dbcon.commit()
            dbcon.close()

