"""SQLファイルを読み込みSQL文を抽出実行するクラスがあるモジュール。
モジュール名はSQLFileExecutor。
"""

import re
import sys
import traceback
import copy
import psycopg2
import psycopg2.extras
import pypostgres

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
    def __init__(self, sql_files, dbcon):
        self.__sql_files = copy.copy(sql_files)
        self.__sql_commands = [];
        self.__dbcon = dbcon
        self._read_sql()

    def _read_sql(self):
        """SQLファイルを読み込みSQL文を抽出する。
        Raises:
            IOError SQLファイルエラー
            Error   SQL文抽出中にエラー
        """
        sqlcoms = '|'.join(SQLFileExecutor.SQLCOMMANDS)
        sqlreg = re.compile(r'(?:{})[ \t]+.*?;'.format(sqlcoms), re.DOTALL)
        sqlcommreg = re.compile(r'^-+.*$')
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
                commands = [sql.strip() for sql in commands]
                self.__sql_commands.append(commands)
            except IOError as ie:
                print("IOError: {} ファイル読み込みエラー SQL文抽出中にエラー".format(fname), 
                        file=sys.stderr)
                raise ie
            except Exception as ex:
                print("Error: {} SQL文抽出中にエラー".format(fname), file=sys.stderr)
                raise ex
            finally:
                if not fin:
                    fin.close()
        print('SQL: ', self.sql_commands)

    def exec(self):
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
    def sql_files(self):
        """SQLファイル名のリストを返す。
        Returns: 
            list[str]: SQLファイル名のリスト
        """
        return copy.copy(self.__sql_files)

    @property
    def sql_commands(self):
        """SQLコマンドのリストを返す
        Returns:
            list[str]: SQLコマンドのリスト
        """
        return copy.copy(self.__sql_commands)


