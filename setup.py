from typing import Optional, Union, Any
from typing import Callable, NoReturn
from typing import Sequence, Iterable, List, Tuple
from typing import Dict
from typing import TypeVar, Generic, NewType, Type
from setuptools import setup, find_packages
import os
import re

# plist = find_packages('src')
# print(plist)

# 依存しているパッケージのリストファイルrequirements.txt
requirements_text: str = 'requirements.txt'
def get_requirements(fname: str) -> list[str]:
    """このパッケージが依存しているパッケージのリストをrequirements.txtから取得して返す。
    Args:
        fname (str): requirements.txt
    Returns:
        依存しているパッケージのリスト
    """
    with open(fname) as fp:
        lines = fp.readlines()
    requires = [line.strip() for line in lines]
    return requires

# バージョン・製作者・Emain・URLなどをライブラリの__init__.pyから取得する
package_name = 'fsqlexec'
root_dir = 'src'
with open(os.path.join(root_dir, 'fsqlexec', '__init__.py'), 'r') as fp:
    init_text = fp.read()
    version = re.search(r'__version__\s*=\s*[\'\"](.+?)[\'\"]', init_text).group(1)
    license = re.search(r'__license__\s*=\s*[\'\"](.+?)[\'\"]', init_text).group(1)
    author = re.search(r'__author__\s*=\s*[\'\"](.+?)[\'\"]', init_text).group(1)
    author_email = re.search(r'__author_email__\s*=\s*[\'\"](.+?)[\'\"]', init_text).group(1)
    url = re.search(r'__url__\s*=\s*[\'\"](.+?)[\'\"]', init_text).group(1)

setup(
    # パッケージ名
    name=package_name,
    # バージョン
    version=version,
    # パッケージのリスト
    # パッケージのソースがsrcにある場合はfind_packages('src')とする。
    packages=find_packages(root_dir),
    # パッケージがあるディレクトリを指定
    # srcがルートソースディレクトリなら{'', 'src'}とする。
    package_dir={'': root_dir},

    # 作者・プロジェクト情報
    author=author,
    author_email=author_email,
    # プロジェクトのホームページのURL
    url=url,

    # 短い説明文と長い説明文を用意
    # content_typeは下記のいずれか
    #   text/plain, text/x-rst, text/markdown
    description='SQLファイルからSQLをまとめて実行する',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',

    #pythonのバージョンは3.6以上で4未満
    python_requires='~=3.9',

    #PyPI上で検索閲覧される情報
    # OS, ライセンス, Pythonバージョン
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Operating System :: OS Independent',
    ],

    # 依存するパッケージ
    # 1.x以上のパッケージをインストールする場合 ==1.*
    # 1.3以上で1.xの最新版をインストールする場合 >=1.3 ~=1.3
    install_requires=get_requirements(requirements_text),

    # *.py以外のデーターファイルを含める
    # package_data = {'パッケージ名': ['データーファイルのパス']}
    # データーファイルのパスは相対パス
    package_data={'SQLFileExecutor': ['conf/*']},

    # エントリーポイント
    entry_points={
        'console_scripts':[
            'fsqlexec = fsqlexec:main',
        ],
    },
)



