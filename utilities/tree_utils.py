"""
スラッシュで区切られた文字列リストからDataFrameを作成するユーティリティ
"""
from utilities.common import SLASH
from typing import List
import pandas as pd

class TreeDF:
  """
  スラッシュで区切られた文字列リストからDataFrameを作成するクラス
  """
  def __init__(self, paths: List[str] = None, target_field: str = "dept", separator: str = SLASH):
    """
    コンストラクタ
    
    Args:
      paths: スラッシュで区切られた文字列のリスト
      target_field: コードとして使用するフィールド名
      separator: パスの区切り文字
    """
    self.separator = separator
    self.target_field = target_field
    self.df = pd.DataFrame({
      'id': pd.Series(dtype='int32'),
      'code': pd.Series(dtype='category'),
      'name': pd.Series(dtype='category'),
      'parent_id': pd.Series(dtype='int32'),
      'level': pd.Series(dtype='int8'),
      'path': pd.Series(dtype='category')
    })
    
    if paths is not None:
      self.paths_to_df(paths, self.target_field)
  
  # ツリー構造をDataFrameに変換
  def to_df(self) -> pd.DataFrame:
    return self.df
  
  def paths_to_df(self, paths: List[str], target_field: str = "dept") -> pd.DataFrame:
    """
    スラッシュで区切られた文字列リストからDataFrameを生成
    
    Args:
      paths: スラッシュで区切られた文字列のリスト
      target_field: コードとして使用するフィールド名
    """
    # パスの前処理
    processed_paths = []
    for path in paths:
      if len(path.strip()) == 0:
        continue
      path = self.separator.join(filter(None, path.strip(self.separator).split(self.separator)))
      if len(path) > 0:
        processed_paths.append(path)
    
    if len(processed_paths) == 0:
      return self.df
    
    # すべてのノードを収集
    all_nodes = {}
    for path in processed_paths:
      parts = path.split(self.separator)
      for i, part in enumerate(parts):
        if part not in all_nodes:
          all_nodes[part] = {
            'code': part,
            'name': part,
            'level': i + 1,
            'path': self.separator.join(parts[:i+1])
          }
    
    # ノードIDのマッピングを作成
    node_id_map = {code: idx + 1 for idx, code in enumerate(all_nodes.keys())}
    
    # DataFrameの作成
    for code, node in all_nodes.items():
      # 
      parts = node['path'].split(self.separator)
      parent_code = parts[-2] if len(parts) > 1 else None
      parent_id = 0 if parent_code is None else node_id_map[parent_code]
      
      # ノードをDataFrameに追加
      self.df = pd.concat([
        self.df,
        pd.DataFrame([{
          'id': node_id_map[code],
          'code': node['code'],
          'name': node['name'],
          'parent_id': parent_id,
          'level': node['level'],
          'path': node['path']
        }])
      ], ignore_index=True)
    
    return self.df
  
  def display_tree(self, show_code: bool = True, printer=print) -> None:
    """
    ツリー構造を視覚的に表示
    
    Args:
      show_code: コードを表示するかどうか
    """
    def display_node(node_id: int, prefix: str = '', is_last: bool = True) -> None:
      """ノードを表示"""
      node = self.df[self.df['id'] == node_id].iloc[0]
      children = self.df[self.df['parent_id'] == node_id]
      
      # 現在のノードの表示
      current_prefix = prefix + ('└─ ' if is_last else '├─ ')
      node_display = f"{node['code']} ({node['name']})" if show_code else node['name']
      printer(current_prefix + node_display)
      
      # 子ノードの表示
      child_prefix = prefix + ('   ' if is_last else '│   ')
      for i, (_, child) in enumerate(children.iterrows()):
        display_node(child['id'], child_prefix, i == len(children) - 1)
    
    # ルートノードを表示
    roots = self.df[self.df['parent_id'] == 0]
    for i, (_, root) in enumerate(roots.iterrows()):
      display_node(root['id'], '', i == len(roots) - 1)

# 使用例
if __name__ == "__main__":
  paths = ['A/B/C', 'A/B/D', 'A/E/F', 'X/Y/Z', 'X/Y/W']
  tree_df = TreeDF(paths)
  print("DataFrameの内容:")
  print(tree_df.df[['id', 'code', 'name', 'parent_id', 'level', 'path']])
  print("\nツリー構造の表示:")
  tree_df.display_tree() 