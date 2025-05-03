# %%
from pathlib import Path as PathBase 
from functools import reduce 
from datetime import date, datetime
import pandas as pd 
import numpy as np 
import os 
import sys 
import time 
import csv
from dateutil.relativedelta import relativedelta 
from typing import Iterable, Self
log = print 
delta = relativedelta 

SP, DOT, COMMA, COLON, SEMICOLON, SLASH, BACKSLASH, BACKQUOTE = ' ', '.', ',', ':', ';', '/', '\\', '`' 
PLUS, MINUS, ASTERISK, SLASH, CARET = '+', '-', '*', '/', '^' 
ATSIGN, PERCENT, AMPERSAND, ASTERISK, DOLLAR = '@', '%', '&', '*', '$' 
UNDERSCORE, TILDE, HASH, VBAR, QUESTION = '_', '~', '#', '|', '?' 
EQUAL, NOT, LT, GT, LTE, GTE = '=', '!=', '<', '>', '<=', '>='
EQ, SQ, DQ = "=", "'", '"' 
UNDER, PER, BAR, VBAR, CARET, TILDE, HASH, = "_", "/", "-", "|", "^", "~", "#"
SHARP, ASTERISK, DOLLAR, AT, PERCENT, AMPERSAND = "#", "*", "$", "@", "%", "&" 
PARENS, BRACKETS, BRACES = "()", "[]", "{}"
LEFT, RIGHT, TOP, BOTTOM = "←", "→", "↑", "↓"

def join(s: any, *ss, sep=SP, func=str) -> str: 
  """
  文字列を結合して返す。
  """
  if isinstance(s, str) == False and isinstance(s, Iterable):
    ss = [*s, *ss]
  else:
    ss = [s, *ss]
  return sep.join(func(x) for x in ss)

def sand(s, a, b=None): 
  """
  文字列を指定した文字列で挟んで返す。
  """
  return join(a, s, a if b is None else b, sep="")

# log(join("aaa", "b", "c"))
# log(join(111, "b", "c"))
# log(join((234, "b") , "c"))

def sq(s): return sand(s, SQ)
def dq(s): return sand(s, DQ) 
def paren(s): return sand(s, *PARENS)
def bracket(s): return sand(s, *BRACKETS)
def brace(s): return sand(s, *BRACES)
def comma(s, *ss, **aa): return join(s, *ss, sep=COMMA, **aa)
def bar(s, *ss, **aa): return join(s, *ss, sep=BAR, **aa)
def under(s, *ss, **aa): return join(s, *ss, sep=UNDER, **aa)
def per(s, *ss, **aa): return join(s, *ss, sep=PER, **aa)
def dot(s, *ss, **aa): return join(s, *ss, sep=DOT, **aa)
def and_(s, *ss, **aa): return join(s, *ss, sep=" and ", func=paren, **aa)
def or_(s, *ss, **aa): return join(s, *ss, sep=" or ", func=paren, **aa)
def xor(s, *ss, **aa): return join(s, *ss, sep=" xor ", func=paren, **aa)
def not_(s, *ss, **aa): return join(s, *ss, sep=" not ", func=paren, **aa)

# log(sq("a"))
# log(dq("a"))
# log(paren("a"))
# log(bracket("a"))
# log(brace("a"))
# log(comma("a", "b", "c", func=sq))
# log(bar("a", "b", "c", func=sq))
# log(under("a", "b", "c", func=sq))
# log(per("a", "b", "c", func=sq))
# log(dot("a", "b", "c", func=sq))
# log(and_("a", "b", "c"))
# log(or_("a", "b", "c"))
# log(xor("a", "b", "c"))
# log(not_("a", "b", "c"))

UTF8 = "utf-8"
UTF16 = "utf-16"
UTF8BOM = "utf-8-sig"
SJIS = "cp932"

class Path(PathBase):
  """
  パスを操作するクラス。
  """
  def __init__(self, *ss, **aa):
    super().__init__(*ss, **aa)
  @property 
  def parent(self) -> Self: 
    return type(self)(super().parent)
  @property
  def parents(self) -> list[Self]:
    return [type(self)(x) for x in super().parents]
  def ensure_dir(self) -> Self: 
    super().mkdir(parents=True, exist_ok=True)
    return self 
  def ensure_file(self) -> Self: 
    self.parent.ensure_dir()
    super().touch(exist_ok=True)
    return self 
  def resolve(self, *ss, **aa) -> Self: 
    return type(self)(super().resolve(*ss, **aa))
  def absolute(self) -> Self: 
    return type(self)(super().absolute())
  def relative_to(self, *ss, **aa) -> Self: 
    return type(self)(super().relative_to(*ss, **aa))
  def with_name(self, name: str) -> Self: 
    return type(self)(super().with_name(name))
  def with_stem(self, stem: str) -> Self: 
    return type(self)(super().with_stem(stem))
  def with_suffix(self, suffix: str) -> Self: 
    return type(self)(super().with_suffix(suffix))
  def expanduser(self) -> Self: 
    return type(self)(super().expanduser())
  def expandvars(self) -> Self: 
    return type(self)(super().expandvars())
  def glob(self, pattern: str) -> Iterable[Self]: 
    return (type(self)(x) for x in super().glob(pattern))
  def rglob(self, pattern: str) -> Iterable[Self]: 
    return (type(self)(x) for x in super().rglob(pattern))
  def with_parts(self, *parts: str) -> Self: 
    return type(self)(super().with_parts(*parts))
  def with_parent(self, parent: Self) -> Self: 
    return type(self)(super().with_parent(parent))
  def with_parents(self, n: int) -> Self: 
    return type(self)(super().with_parents(n))
  

def fullpath(path: str | Path | Iterable[str], *paths, **aa) -> Path:
  """
  パスを結合して絶対パスを返す。
  """
  if isinstance(path, str) == False and isinstance(path, Iterable):
    paths = [*path, *paths]
  else:
    paths = [path, *paths]
  root = Path(".")
  full = reduce(lambda a, b: Path(a) / Path(b), paths, root)
  return full.expanduser().resolve().absolute() 

# log(fullpath("..", "hoge", "fuga", "piyo"))
# log(fullpath(("..", "hoge", "fuga"), "piyo"))
    
def load_csv(path: str | Path | Iterable[str], 
             *paths, encoding=UTF8BOM, 
             **aa) -> pd.DataFrame:
  """
  CSVファイルを読み込んでDataFrameを返す。
  """
  fn = fullpath(path, *paths)
  return pd.read_csv(fn, encoding=encoding, **aa)

def save_csv(df: pd.DataFrame, 
             path: str | Path | Iterable[str], *paths, 
             encoding=UTF8BOM, 
             quoting=csv.QUOTE_ALL,
             index=False, 
             **aa) -> Path:
  """
  DataFrameをCSVファイルに保存する。
  """
  fn = fullpath(path, *paths)
  fn.parent.ensure_dir()
  df.to_csv(fn, encoding=encoding, quoting=quoting, index=index, **aa)
  return fn

# df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
# fn = fullpath("test").ensure_dir() / "test.csv"
# save_csv(df, fn)
# log(load_csv(fn))

def num(d: int | float, spec=","): return format(d, spec) 
def ymd(d: date, spec="%Y%m%d"): return d.strftime(spec)
def hms(d: datetime, spec="%H%M%S"): return d.strftime(spec)
def ymds_of(st: date|Iterable[date], ed: date = None, spec="%Y%m%d", sep=BAR) -> str:
  if isinstance(st, Iterable):
    st, ed = st 
  elif ed is None:
    ed = st 
  return join(ymd(st, spec), sep, ymd(ed, spec), sep="")

# log(num(1234567890))
# log(num(1234567890.011, ",.2f"))
# log(ymd(date(2025, 1, 1)))
# log(hms(datetime(2025, 1, 1, 12, 34, 56)))
# log(ymds_of(date(2025, 1, 1), date(2025, 12, 31)))

def step_of(index: int, total: any, sep=PER) -> str:
  """
  インデックスと総数を進捗率として表示する。
  """
  if hasattr(total, "__len__"):
    # 総数がイテラブルの場合は、総数の長さを総数とする。
    total = len(total)
    index = index + 1 
  return join(num(index), num(total), sep=sep)

# log(step_of(1234, 12345))
# log(step_of(1234, range(12345)))

def months_of(st: Iterable[date] | date, ed: date = None, strict=True) -> Iterable[date]:
  """
  開始日と終了日の間の月のブロックを返す。
  ブロックは月の開始日と終了日のペアのタプルで返す。
  strict=Trueの場合、
  開始日と終了日がブロックの中にある場合は、
  ブロックの開始日と終了日を開始日と終了日に変更する。
  """
  if isinstance(st, Iterable):
    st, ed = st 
  elif isinstance(st, date) and ed is None: 
    ed = st 
  stt = st + delta(day=1)
  edd = ed + delta(day=31)
  diff = delta(edd, stt).months 
  log(st, RIGHT, stt)
  log(ed, LEFT, edd)
  log(diff)
  months = list(
    tuple(stt + delta(months=x, day=y) 
     for y in (1, 31)) 
     for x in range(diff + 1)
     )
  if strict:
    # 開始日と終了日がブロックの中にある場合は、
    # ブロックの開始日と終了日を開始日と終了日に変更する。  
    months[0] = (max(st, stt), months[0][-1])
    months[-1] = (months[-1][0], min(ed, edd))
  return months 

# log(months_of(date(2025, 1, 1), date(2025, 12, 31)))
# log(months_of(date(2025, 1, 1)))
# log(months_of((date(2025, 1, 1), date(2025, 12, 31))))

FISCAL_YEAR_START = 4 
def fiscal_year_of(d: date=date.today(), start=FISCAL_YEAR_START, end=12) -> tuple[date, date]:
  """`
  会計年度の開始日と終了日を返す。
  会計年度は4月から始まる。
  会計年度の終了日は3月31日。
  """
  year = d.year - (1 if d.month < start else 0)
  st = date(year, start, 1)
  ed = date(year + 1, end, 31)
  return st, ed 

# for d in (date.today() + delta(months=x) for x in range(14)):
#   log(d, fiscal_year_of(d))



