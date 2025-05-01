# %%
# 標準・外部ライブラリのインポート
from pathlib import Path as PathBase
from typing import List, Optional, Tuple, Union, Self
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from functools import reduce
import pandas as pd
import csv

# relativedeltaの短縮名
delta = relativedelta

# ログ出力用（デフォルトはprint。必要に応じて差し替え可能）
log = print

class Path(PathBase):
  """
  pathlib.Pathを拡張した独自Pathクラス。
  追加メソッドにより、メソッドチェーンや型アノテーションの互換性を向上。
  """
  def __init__(self, *a, **aa):
    super().__init__(*a, **aa)

  @property
  def parent(self) -> Self:
    """親ディレクトリを同型で返す（型安全性向上）"""
    return type(self)(super().parent)

  @property
  def parents(self) -> list[Self]:
    """全親ディレクトリを同型のリストで返す"""
    return list(type(self)(x) for x in super().parents)

  def mkdir(self, exists_ok=True, parents=True, *a) -> Self:
    """
    ディレクトリを作成し、自身を返す。
    既存ディレクトリがあってもエラーにならない（デフォルト）。
    """
    super().mkdir(parents=parents, exist_ok=exists_ok, *a)
    return self

  def touch(self, exist_ok=True, **a) -> Self:
    """
    ファイルを作成し、自身を返す。
    親ディレクトリがなければ自動作成。
    """
    self.parent.mkdir()
    super().touch(exist_ok=exist_ok, **a)
    return self

  def glob(self, pattern: str, **a) -> list[Self]:
    """
    パターンにマッチするファイル・ディレクトリを同型でリスト化。
    """
    return [type(self)(x) for x in super().glob(pattern, **a)]

  def expanduser(self):
    """~を展開し、同型で返す"""
    return type(self)(super().expanduser())

  def ensure_dir(self) -> Self:
    """
    ディレクトリがなければ作成し、自身を返す。
    存在確認と作成を一括で行う。
    """
    return self.mkdir() if not self.exists() else self

  def relative_to(self, other: Self) -> Self:
    """
    指定されたパスからの相対パスを計算し、同型で返す。
    """
    return type(self)(super().relative_to(other))

def fullpath(*path: list[str | Path]) -> Path:
  """
  複数のパス要素を結合し、絶対パス（Path型）として返す。
  文字列・Path混在可。~展開・絶対化も自動。
  """
  root = Path(".")
  full = reduce(lambda a, b: Path(a) / Path(b), path, root)
  return full.expanduser().resolve(strict=False).absolute()

# 文字コード定数（日本語WindowsやExcelとの互換性も考慮）
UTF8 = "utf-8"
UTF8_BOM = "utf-8-sig"
SJIS = "cp932"

def save_csv(
  df: pd.DataFrame, *path: list[str | Path], encoding=UTF8_BOM, quoting=csv.QUOTE_ALL, index=False, **kwargs
) -> Path:
  """
  DataFrameをCSVファイルとして保存し、保存先のPathを返す。
  デフォルトでBOM付きUTF-8、Excel互換の設定。
  """
  fn = fullpath(*path)
  df.to_csv(fn, encoding=encoding, quoting=quoting, index=index, **kwargs)
  return fn

def load_csv(
  *path: list[str | Path], encoding=UTF8_BOM, **kwargs
) -> pd.DataFrame:
  """
  CSVファイルを読み込み、DataFrameとして返す。
  デフォルトでBOM付きUTF-8、Excel互換の設定。
  """
  fn = fullpath(*path)
  df = pd.read_csv(fn, encoding=encoding, **kwargs)
  return df

# よく使う記号の定数（ファイル名や文字列操作の互換性向上）
SP, DOT, COMMA, PER, BAR, UNDER = " ", ".", ",", "/", "-", "_"
EQUAL, COLON, SEMICOLON, AT, DOLLER, PERCENT = "=", ":", ";", "@", "$", "%"
AMP, PLUS, MINUS, ASTERISK, SLASH = "&", "+", "-", "*", "/"
STAR, EXCLAMATION, TILDE, CARET, DOLLAR = "*", "!", "~", "^", "$"
SHARP, BACKSLASH = "#", "\\"
PAREN, BRACKET, BRACES = "()", "[]", "{}"
SQ, DQ = "'", '"'
LEFT, BOTTOM, UP, RIGHT = "←↓↑→"
CR, LF = "\r", "\n"

# 文字列結合や装飾のためのユーティリティ関数群
def join(*ss: list[str], sep=SP, func=str) -> str:
  """指定区切りで文字列を結合。型変換も可能。"""
  return sep.join(func(x) for x in ss)

def sand(s, a, b=None, **aa) -> str:
  """両端に記号を挟んで結合。b未指定時はaで両端。"""
  return join(a, s, a if b is None else b, sep="", **aa)

def sq(s): return sand(s, SQ)
def dq(s): return sand(s, DQ)
def paren(s): return sand(s, *PAREN)
def bracket(s): return sand(s, *BRACKET)
def braces(s): return sand(s, *BRACES)
def comma(*ss): return join(*ss, sep=COMMA)
def dot(*ss): return join(*ss, sep=DOT)
def per(*ss): return join(*ss, sep=PER)
def bar(*ss): return join(*ss, sep=BAR)
def under(*ss): return join(*ss, sep=UNDER)
def and_(*ss, func=paren): return join(*ss, sep=" and ", func=func)
def or_(*ss, func=paren): return join(*ss, sep=" or ", func=func)

def num(d, spec=","): return format(d, spec)
def ymd(d, spec="%Y%m%d"): return format(d, spec)
def hms(d, spec="%H%M%S"): return format(d, spec)
def ymds_of(*dd, sep=BAR, spec="%Y%m%d") -> str:
  """
  複数の日付を指定形式で結合。
  例: ymds_of(date(2023, 1, 1), date(2023, 12, 31), spec="%Y/%m/%d")
  """
  return join(*[format(d, spec) for d in dd], sep=sep)

if __name__ == "__main__":
  # テスト用のコードブロック
  log(ymds_of(date(2023, 1, 1), date(2023, 12, 31)))
  log(ymds_of(date(2023, 1, 1), date(2023, 12, 31), spec="%Y/%m/%d"))
  # 例: ymds_of(date(2023, 1, 1), date(2023, 12, 31), spec="%Y/%m/%d")

def step_of(index, total):
  """
  進捗表示のためのユーティリティ。
  指定されたインデックスと総数から、進捗率を計算して返す。
  """
  # if len(total) is available, total is a list
  if hasattr(total, "__len__"):
    total = len(total)
    index = index + 1
  return per(num(index), num(total))

def month_range(st: date, ed: date) -> list[Tuple[date, date]]:
  """
  2つの日付の間の各月の（開始日, 終了日）のリストを返す。
  月をまたぐ集計やレポート作成時に便利。
  """
  stt = min(st + delta(day=1), st)
  edd = max(ed + delta(day=31), ed)
  diff = delta(edd, stt).months
  months = list(list(stt + delta(months=x, day=y) for y in (1, 31)) for x in range(diff + 1))
  months[0] = (max(stt, st), months[0][-1])
  months[-1] = (months[-1][0], min(ed, edd))
  return months

def fiscal_year(day: date) -> Tuple[date, date]:
  """
  指定日付が属する会計年度（4月始まり）の開始日と終了日を返す。
  日本の会計年度処理などに対応。
  """
  fy = day.year + (day.month > 3)
  st = date(fy, 4, 1)
  ed = date(fy + 1, 3, 31)
  return st, ed

if False:
  st = date(2025, 8, 10)
  for day in (st + delta(months=x) for x in range(14)):
    log(day, RIGHT, fiscal_year(day))