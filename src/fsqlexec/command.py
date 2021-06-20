"""SQLファイルを読み込みSQL文を抽出実行するコマンドがあるモジュール。
モジュール名はfsqlexec
"""

from typing import Optional, Union, Any
from typing import NoReturn
from typing import Dict
from typing import TypeVar, Generic, NewType, Type, ClassVar
from typing import IO, TextIO, BinaryIO
from collections.abc import Callable
from collections.abc import Sequence, Iterable

import sys
from pathlib import Path
from db import pypostgres
from fsqlexec.SQLFileExecutor import SQLFileExecutor
import click
from logging import getLogger

logger = getLogger(__name__)

def check_file_list_exists(files: Iterable[str]) -> bool:
    """引数のSQLファイルのリストが存在するか確認する。
    SQLファイルが全て存在する場合は何もしない。
    存在しない場合は例外IOErrorを送出する。
    Raises:
        IOError: ファイルが存在しない
    Returns:
        True: 全てのファイルは存在する
    """
    for fname in files:
        path = Path(fname)
        if not path.exists():
            raise IOError(f'SQLファイル[{fname}]が存在しません。')
    return True

def fname_line_to_array(fname: Optional[str]) -> list[str]:
    """ファイルから行を取得それを配列にして返す。
    Args:
        fname (str): ファイル名 
    Raises:
        IOError: ファイルが存在しない
                    ファイル読み込みエラー
    Returns:
        list[str]: ファイルのリスト
                    引数のファイル名がNoneの場合は空の配列
    """
    if fname is None:
        return []

    path = Path(fname)
    if not path.exists():
        raise IOError(f'Excludeファイル[{fname}]が存在しません。')
    with open(fname, "r") as fp:
        ret = fp.readlines()
        ret = [line.rstrip() for line in ret]
    return ret

def create_sql_files(include_file: Sequence[str], exclude_file: str) -> list[str]:
    """include_fileのファイルリストからexclude_file除外ファイルのファイルを削除する。
    Args:
        include_file (Sequence[str]): ファイルのリスト
        exclude_file (str): 除外するファイルのリスト
    Returns:
        list[str]: 除外したファイルのちのinclude_fileのリスト
    Raises:
        IOError:    
                    exclude_fileが存在しない
                    ファイル読み込みエラー
    """
    excludes = fname_line_to_array(exclude_file)
    ret = [fname for fname in include_file if fname not in excludes]
    check_file_list_exists(ret)
    return ret

def create_SQLFileExecutor(sql_files: Sequence[str], ini_file: str, error_exec: bool) -> SQLFileExecutor:
    """SQLFileExecutorオブジェクトを作成して返す。
    Args:
        sql_files (Sequence[str]): SQLファイルのリスト
        ini_file (str): DB接続情報のiniファイル
    Raises:
        IOError: iniファイル読み込みエラー
        SQLException: DB接続エラー
    Returns:
        SQLFileExecutor: SQLFileExecutor
    """
    # DB接続
    dbcon = pypostgres.get_config_connection(ini_file, 'PostgreSQL')
    logger.debug("DB接続" + str(dbcon))
    sqlexec = SQLFileExecutor(sql_files, dbcon, error_exec)
    return sqlexec

@click.command()
@click.option("--exclude-file", "exclude_file", type=str, default=None, help="Exclude SQL file")
@click.option("--ini-file", "ini_file", type=str, default="postgres.ini", help="DB接続.iniファイル")
@click.option("--error_exec", "error_exec", is_flag=True, help="エラーが起きても処理を継続する")
@click.argument("sql_files", nargs=-1)
def cmd(exclude_file: str, ini_file: str, sql_files: Sequence[str], error_exec: bool) -> None:
    sqlexec = None
    try:
        logger.debug("ini file: " + ini_file)
        logger.debug("exclude_file: " + str(exclude_file))
        logger.info("SQL File: " + str(sql_files))
        sql_files = create_sql_files(sql_files, exclude_file)
        logger.info("Exec SQL File: " + str(sql_files))
        sqlexec = create_SQLFileExecutor(sql_files, ini_file, error_exec)
        sqlexec.exec()
        logger.info("SQL実行終了")
    except IOError as ie:
        logger.error(ie)
        sys.exit(2)
    except Exception as ex:
        print(ex)
        logger.error(ex)
        sys.exit(3)
    finally:
        if sqlexec is not None:
            sqlexec.close()



def main() -> None:
    cmd()

# *importでimportするクラス・関数
__all__ = [
    "check_file_list_exists", "fname_line_to_array", 
    "create_sql_files", "create_SQLFileExecutor",
    "cmd", "main"
]

if __name__ == '__main__':
    main()
