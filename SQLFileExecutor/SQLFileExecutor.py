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
from db import pypostgres

# DB接続オブジェクト
C = TypeVar('C')
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
    def __init__(self, sql_files: list[str], dbcon: C):
        self.__sql_files: Sequence[str] = copy.copy(sql_files)
        self.__sql_commands: Sequence[Sequence[str]]= [];
        self.__dbcon: C = dbcon
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
        # セミコロンと改行を削除するラムダ関数
        delfn = lambda sql: sql.replace("\n", " ").replace(";", "")
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
                print("Error: {} SQL文抽出中にエラー".format(fname), file=sys.stderr)
                raise ex
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
                    except psycopg2.InternalError as ie:
                        print('Error: SQLFile={file},SQL {sql}'.format(file=self.sql_files[idx],
                            sql=sql))
                        print(traceback.format_exc())
                        raise ie
                    except psycopg2.Error as ex:
                        print('Error: SQLFile={file},SQL {sql}'.format(file=self.sql_files[idx],
                            sql=sql))
                        print(traceback.format_exc())
                        raise ex
        except psycopg2.Error as ex:
            print('Error: cursor取得エラー dbcon={}'.format(str(dbcon)))
            raise ex
        finally:
            # エラーが起きたらrollback()される
            dbcon.commit()
            cur.close()
    
    @property
    def sql_files(self) -> list[str]:
        """SQLファイル名のリストを返す。
        Returns: 
            list[str]: SQLファイル名のリスト
        """
        return copy.copy(self.__sql_files)

    @property
    def sql_commands(self) -> list[list[str]]:
        """SQLコマンドのリストを返す
        Returns:
            list[str]: SQLコマンドのリスト
        """
        return copy.copy(self.__sql_commands)

import sys
from pathlib import Path
from db import pypostgres

def check_file_list_exists(files: Iterable[str]) -> None:
    """引数のSQLファイルのリストが存在するか確認する。
    SQLファイルが全て存在する場合は何もしない。
    存在しない場合は例外IOErrorを送出する。
    Raises:
        IOError: ファイルが存在しない
    """
    for fname in files:
        path = Path(fname)
        if not path.exists():
            raise IOError(f'SQLファイル[{fname}]が存在しません。')

def main() -> None:
    dbcon = None
    try:
        # DB接続
        ini_file = "conf/postgres.ini"
        dbcon = pypostgres.get_config_connection(ini_file, 'PostgreSQL')
        print("DB接続", dbcon)

        sql_files = sys.argv[1:]
        print(sql_files)
        check_file_list_exists(sql_files)
        sqlexec = SQLFileExecutor(sql_files, dbcon)
        sqlexec.exec()
        print("SQL実行終了")
    except IOError as ie:
        print(ie)
        sys.exit(2)
    except Exception as ex:
        print(ex)
        sys.exit(3)
    finally:
        print("SQL COMMITとDB切断")
        if dbcon is not None:
            dbcon.commit()
            dbcon.close()

if __name__ == '__main__':
    main()
