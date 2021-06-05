"""SQLファイルを読み込みSQL文を抽出実行するクラスがあるモジュール。
モジュール名はSQLFileExecutor。
"""

from typing import Optional, Union, Any
from typing import NoReturn
from typing import Dict
from typing import TypeVar, Generic, NewType, Type, ClassVar
from typing import IO, TextIO, BinaryIO
from collections.abc import Callable
from collections.abc import Sequence, Iterable

import re
import sys
import traceback
import copy
import psycopg2
import psycopg2.extras
# import psycopg2.Error
from db import pypostgres
from db.SQLException import SQLException

"""SQLファイルを読み込み、SQL文を抽出しそれをすべて実行するクラス。
まずSQLファイルはSQL文が記述されているファイルで;でSQL文が区切られている必要がある。
この複数のSQLファイルをリストにしてコンストラクタに渡す。
DBはPostgreSQLを使用し、psycopg2のモジュールのコネクタを使用し、コンストラクタで渡す必要がある。

Args:
    sql_files (list[str]):   SQL文のファイル
    dbcon (psycopg2.Connection): DBコネクション
Attributes:
    sql_files (list[str]): 複数のSQLファイルのリスト。
    sql_commands (list[list[str]]): 
        リストのリストでSQLコマンドが格納されており一つのリストはsql_filesのSQLファイルに対応する。

使用方法
dbcon = ...   # DBコネクションオブジェクト
sqlfiles = [...., ..., ...]
ddlexec = SQLFileExecutor(sqlfiles, dbcon)
ddlexec.exec()

コンストラクタでSQLファイルを読み込みSQL文抽出、exec()でSQL文を実行することになる。
"""
class SQLFileExecutor():
    
    """list[str]: 抽出するSQL文の予約語のリスト
    """
    SQLCOMMANDS = ['SELECT', 'INSERT', 'DELETE', 'UPDATE', 'CREATE', 'ALTER', 'DROP']
    
    # コンストラクタ
    def __init__(self, sql_files: Sequence[str], dbcon: Any):
        self.__sql_files: Sequence[str] = copy.copy(sql_files) # type: ignore
        self.__sql_commands: list[list[str]]= [];
        self.__dbcon: Any = dbcon
        self._read_sql()


    def _read_sql(self) -> None:
        """SQLファイルを読み込みSQL文を抽出する。
        Raises:
            IOError SQLファイルエラー
            Error   SQL文抽出中にエラー
        """
        sqlcoms = '|'.join(SQLFileExecutor.SQLCOMMANDS)
        # SQL文の正規表現とSQLのコメントの正規表現
        sqlreg = re.compile(r'(?:{})[ \t]+.*?;'.format(sqlcoms), 
                                flags=re.DOTALL|re.IGNORECASE)
        sqlcommreg = re.compile(r'^-+.*$')

        # セミコロンと改行を削除する内部関数
        def delfn(sql: str) -> str:
            return sql.replace("\n", " ").replace(";", "")

        fin = None
        for fname in self.sql_files:
            try:
                fin = open(fname, 'r')
                contents = fin.read()
                # SQLコメントを削除する
                contents = sqlcommreg.sub('', contents)
                # SQL文の抽出
                commands = sqlreg.findall(contents)
                # 後々処理がしやすいように前後の空白を削除
                # またセミコロンと改行を削除
                commands = [delfn(sql.strip()) for sql in commands]
                self.__sql_commands.append(commands)
                fin = None
            except IOError as ie:
                print("IOError: {} ファイル読み込みエラー SQL文抽出中にエラー".format(fname), 
                        file=sys.stderr)
                raise ie
            except Exception as ex:
                msg = "Error: {} SQL文抽出中にエラー".format(fname) 
                print(msg, file=sys.stderr)
                raise Exception(msg)
            finally:
                if fin is not None:
                    fin.close()
        print('SQL: ', self.sql_commands)

    def exec(self) -> None:
        """抽出したSQL文をすべて実行する。
        SQL文を全てを実行したらコミットされる。
        エラー時はロールバックされる。
        """
        dbcon = self.__dbcon
        cur = None
        try:
            cur = dbcon.cursor(cursor_factory=psycopg2.extras.DictCursor)
            for idx, commands in enumerate(self.sql_commands):
                print('SQLファイル: ', self.sql_files[idx])
                for sql in commands:
                    try:
                        print('SQL: ', sql)
                        cur.execute(sql)
                    except psycopg2.Error as ex:
                        print('Error: SQLFile={file},SQL {sql}'.format(file=self.sql_files[idx],
                            sql=sql))
                        print(traceback.format_exc())
                        raise ex
        except Exception as ex:
            msg = 'Error: SQL実行エラー sql={}'.format(sql)
            print(msg)
            raise SQLException(msg)
        finally:
            # エラーが起きたらrollback()される
            dbcon.commit()
            if cur is not None:
                cur.close()
    
    def close(self) -> None:
        """DB接続を切断する。
        """
        print("SQL COMMITとDB切断")
        dbcon = self.__dbcon
        if dbcon is not None:
            dbcon.commit()
            dbcon.close()


    @property
    def sql_files(self) -> Any:
        """SQLファイル名のリストを返す。
        Returns: 
            list[str]: SQLファイル名のリスト
        """
        ret = copy.copy(self.__sql_files) # type: ignore
        return ret 

    @property
    def sql_commands(self) -> Any:
        """SQLコマンドのリストを返す
        Returns:
            list[str]: SQLコマンドのリスト
        """
        ret = copy.copy(self.__sql_commands) # type: ignore
        return ret


