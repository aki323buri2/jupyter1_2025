"""
ツリー構造とDataFrameの相互変換を行うユーティリティモジュール
"""
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
from datetime import datetime
from pathlib import Path

class TreeDF:
  """
  ツリー構造とDataFrameの相互変換を行うクラス
  """
  def __init__(self):
    self.df = self._create_empty_df()
  
  @staticmethod
  def _create_empty_df() -> pd.DataFrame:
    """空の最適化されたDataFrameを作成"""
    return pd.DataFrame({
      'id': pd.Series(dtype='int32'),
      'code': pd.Series(dtype='category'),
      'name': pd.Series(dtype='category'),
      'parent_id': pd.Series(dtype='int32'),
      'parent_code': pd.Series(dtype='category'),
      'level': pd.Series(dtype='int8'),
      'path': pd.Series(dtype='category'),
      'order': pd.Series(dtype='int16'),
      'is_leaf': pd.Series(dtype='bool'),
      'is_active': pd.Series(dtype='bool'),
      'created_at': pd.Series(dtype='datetime64[ns]'),
      'updated_at': pd.Series(dtype='datetime64[ns]')
    })
  
  def tree_to_df(
    self,
    tree: Dict[str, Any],
    parent_id: Optional[int] = None,
    parent_code: str = '',
    level: int = 1,
    order: int = 1
  ) -> pd.DataFrame:
    """
    ツリー構造から最適化されたDataFrameを生成
    
    Args:
      tree: ツリー構造の辞書
      parent_id: 親ノードのID
      parent_code: 親ノードのコード
      level: 現在の階層レベル
      order: 兄弟間の順序
    
    Returns:
      最適化されたDataFrame
    """
    # 現在のノードの情報を構築
    current_id = len(self.df) + 1
    current_code = tree['dept']
    current_path = f"{parent_code}/{current_code}" if parent_code else current_code
    
    # 新しい行のデータ
    new_row = {
      'id': current_id,
      'code': current_code,
      'name': tree['name'],
      'parent_id': parent_id if parent_id is not None else 0,
      'parent_code': parent_code,
      'level': level,
      'path': current_path,
      'order': order,
      'is_leaf': len(tree['children']) == 0,
      'is_active': True,
      'created_at': datetime.now(),
      'updated_at': datetime.now()
    }
    
    # DataFrameに新しい行を追加
    self.df = pd.concat([
      self.df,
      pd.DataFrame([new_row])
    ], ignore_index=True)
    
    # 子ノードを再帰的に処理
    for i, child in enumerate(tree['children'], 1):
      self.tree_to_df(
        child,
        parent_id=current_id,
        parent_code=current_code,
        level=level + 1,
        order=i
      )
    
    return self.df
  
  def df_to_tree(self, df: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
    """
    DataFrameからツリー構造を生成
    
    Args:
      df: 変換するDataFrame（Noneの場合は内部のdfを使用）
    
    Returns:
      ツリー構造の辞書
    """
    if df is None:
      df = self.df
    
    # ルートノードを取得
    root = df[df['parent_id'] == 0].iloc[0]
    
    def build_node(node_id: int) -> Dict[str, Any]:
      """指定されたIDのノードとその子孫を構築"""
      node = df[df['id'] == node_id].iloc[0]
      children = df[df['parent_id'] == node_id].sort_values('order')
      
      return {
        'dept': node['code'],
        'name': node['name'],
        'id': node['id'],
        'children': [build_node(child_id) for child_id in children['id']]
      }
    
    return build_node(root['id'])
  
  def validate(self) -> bool:
    """DataFrameの整合性を検証"""
    # 親IDの存在確認
    valid_parents = (self.df['parent_id'] == 0) | self.df['parent_id'].isin(self.df['id'])
    if not valid_parents.all():
      return False
    
    # パスの整合性確認
    for _, row in self.df.iterrows():
      if row['parent_id'] != 0:
        parent = self.df[self.df['id'] == row['parent_id']].iloc[0]
        if not row['path'].startswith(parent['path'] + '/'):
          return False
    
    # レベルの整合性確認
    for _, row in self.df.iterrows():
      if row['parent_id'] != 0:
        parent = self.df[self.df['id'] == row['parent_id']].iloc[0]
        if row['level'] != parent['level'] + 1:
          return False
    
    return True
  
  def optimize_memory(self) -> None:
    """DataFrameのメモリ使用量を最適化"""
    self.df = self.df.astype({
      'id': 'int32',
      'parent_id': 'int32',
      'level': 'int8',
      'order': 'int16',
      'code': 'category',
      'name': 'category',
      'parent_code': 'category',
      'path': 'category',
      'is_leaf': 'bool',
      'is_active': 'bool'
    })
  
  def save_to_csv(self, path: Path) -> None:
    """DataFrameをCSVファイルとして保存"""
    self.df.to_csv(path, index=False)
  
  def load_from_csv(self, path: Path) -> None:
    """CSVファイルからDataFrameを読み込み"""
    self.df = pd.read_csv(path)
    self.optimize_memory()

# ユーティリティ関数
def get_children(df: pd.DataFrame, node_id: int) -> pd.DataFrame:
  """指定ノードの子ノードを取得"""
  return df[df['parent_id'] == node_id]

def get_descendants(df: pd.DataFrame, node_id: int) -> pd.DataFrame:
  """指定ノードの子孫ノードを取得"""
  node = df[df['id'] == node_id].iloc[0]
  return df[df['path'].str.startswith(node['path'] + '/')]

def get_ancestors(df: pd.DataFrame, node_id: int) -> pd.DataFrame:
  """指定ノードの祖先ノードを取得"""
  node = df[df['id'] == node_id].iloc[0]
  path_parts = node['path'].split('/')
  ancestor_paths = ['/'.join(path_parts[:i+1]) for i in range(len(path_parts)-1)]
  return df[df['path'].isin(ancestor_paths)]

def get_siblings(df: pd.DataFrame, node_id: int) -> pd.DataFrame:
  """指定ノードの兄弟ノードを取得"""
  node = df[df['id'] == node_id].iloc[0]
  return df[(df['parent_id'] == node['parent_id']) & (df['id'] != node_id)]

# 使用例
if __name__ == "__main__":
  # サンプルのツリー構造
  sample_tree = {
    'dept': 'A',
    'name': '総務',
    'id': 1,
    'children': [
      {
        'dept': 'B',
        'name': '人事',
        'id': 2,
        'children': [
          {
            'dept': 'D',
            'name': '採用',
            'id': 4,
            'children': []
          },
          {
            'dept': 'E',
            'name': '教育',
            'id': 5,
            'children': []
          }
        ]
      },
      {
        'dept': 'C',
        'name': '経理',
        'id': 3,
        'children': []
      }
    ]
  }
  
  # TreeDFインスタンスの作成
  tree_df = TreeDF()
  
  # ツリーからDataFrameへの変換
  df = tree_df.tree_to_df(sample_tree)
  
  # 結果の表示
  print(df[['id', 'code', 'name', 'parent_id', 'level', 'path', 'is_leaf']])
  
  # DataFrameからツリーへの逆変換
  reconstructed_tree = tree_df.df_to_tree()
  
  # 整合性の検証
  is_valid = tree_df.validate()
  print(f"DataFrame is valid: {is_valid}")
  
  # メモリ最適化
  tree_df.optimize_memory()
  
  # CSVファイルへの保存
  tree_df.save_to_csv(Path('tree_data.csv'))
  
  # CSVファイルからの読み込み
  tree_df.load_from_csv(Path('tree_data.csv')) 