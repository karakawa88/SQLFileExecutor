# fsqlexecライブラリ

## fsqlexecの概要
SQLファイルを読み込みSQLを実行するパッケージ。
パッケージ名はfsqlexecである。
それを実行するコマンドも付属しコマンド名はfsqlexecである。
PyPIのライブラリでライブラリ名はfsqlexecである。

## 構成
fsqlexecパッケージはSQLファイルを読み込みSQLを実行するモジュールがある
パッケージとそれを実行するコマンドで構成されている。
モジュール名はSQLFileExecutorでコマンド名はfsqlexecである。

## インストール
```shell
pip3 install fsqlexec
```

## SQLFileExecutorモジュール
SQLファイルを読み込みSQLを実行するモジュール。
fsqlexecパッケージのSQLFileExecutorモジュールのSQLFileExecutorクラスがこれを行う。
使用方法
```python
from fsqlexec.SQLFileExecutor import SQLFileExecutor

sql_files = [ ... ]     # SQLファイル名の配列
dbcon = ...             # DBコネクション
error_exec = TRUE       # エラーがあっても処理を継続するか
fsqlexec = SQLFileExecutor(sqlfiles, dbcon, error_exec)
fsqlexec.exec()
```
SQLファイルとコマンドを取得できエラーがあった場合の情報をこのクラスのオブジェクトは持つ。
```python
sql_files = fsqlexec.sql_files
commends = fsqlexec.sql_commands
errors = fsqlexec.errors
```
エラー情報は一つのエラー情報が辞書の配列である。
sql_file    SQLファイル名
sql         SQL文
exception   例外オブジェクト

## fsqlexecコマンド
このSQLファイルを読み込みSQL文を実行するコマンドがfsqlexecである。
パッケージのインストールと同時に配置される。
使い方
```shell
fsqlexec --exclude-file 除外するSQLファイル --ini-file DB接続情報 --error-exec SQLファイル....
```
オプション
--exclude-file
除外するSQLファイルが記述されたファイル。
ファイルの書式はファイル名を一行にして記述。
--ini-file
DB接続情報のファイル指定。
DB接続情報
```txt
[PostgreSQL]
host=ホスト名
dbname=DB名
user=ユーザー名
password=パスワード
```
--error-exec
エラーがあっても処理を継続するか。
指定しない場合はエラーがあったら即処理中断。

##fsqlexecライセンス
このソフトウェアはMITライセンスを適用している。
This software is released under the MIT License, see LICENSE.txt.
（このソフトウェアは、MITライセンスのもとで公開されている。LICENSE.txtを見よ。）

