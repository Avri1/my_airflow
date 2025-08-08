"""
简单的 NoSQL 数据库模拟实现
使用 Python 的字典和类来模拟数据库操作
"""

import json
from typing import Dict, Any, Tuple, Optional
import threading
import copy


class NoSQLClient:
    """模拟 NoSQL 数据库客户端

    说明：为了支持在 Airflow/Knative 的跨进程场景中通过 XCom/JSON 传递“状态副本”，
    在原有基于进程内单例的实现外，新增了基于“纯字典状态”的无类 API（见文末的
    init_state/insert/update/get/delete）。这样业务侧只需多传一个字典参数即可在不同
    Pod 之间传递一致的状态。
    """
    
    def __init__(self):
        # 使用字典来模拟数据库表
        # 结构: {table_name: {primary_key_value: {secondary_key_value: record_data}}}
        self._tables: Dict[str, Dict[str, Dict[str, Dict[str, Any]]]] = {}
        # 线程锁，确保数据操作的安全性
        self._lock = threading.Lock()
        
    def insert(self, table_name: str, primary_key: Tuple[str, str], 
               secondary_key: Tuple[str, str], data: Dict[str, Any]) -> None:
        """
        插入数据到指定表
        
        Args:
            table_name: 表名
            primary_key: 主键，格式为 (键名, 键值)
            secondary_key: 副键，格式为 (键名, 键值)
            data: 要插入的数据字典
        """
        with self._lock:
            # 确保表存在
            if table_name not in self._tables:
                self._tables[table_name] = {}
            
            # 确保主键存在
            pk_name, pk_value = primary_key
            if pk_value not in self._tables[table_name]:
                self._tables[table_name][pk_value] = {}
            
            # 插入数据
            sk_name, sk_value = secondary_key
            record = copy.deepcopy(data)
            record[pk_name] = pk_value
            record[sk_name] = sk_value
            
            self._tables[table_name][pk_value][sk_value] = record
            
            print(f"成功插入到 {table_name}: {pk_name}={pk_value}, {sk_name}={sk_value}")
    
    def update(self, table_name: str, primary_key: Tuple[str, str], 
               secondary_key: Tuple[str, str], update_data: Dict[str, Any]) -> None:
        """
        更新指定记录
        
        Args:
            table_name: 表名
            primary_key: 主键，格式为 (键名, 键值)
            secondary_key: 副键，格式为 (键名, 键值)
            update_data: 要更新的数据字典
        """
        with self._lock:
            pk_name, pk_value = primary_key
            sk_name, sk_value = secondary_key
            
            # 检查表和记录是否存在
            if (table_name not in self._tables or 
                pk_value not in self._tables[table_name] or
                sk_value not in self._tables[table_name][pk_value]):
                print(f"警告: 记录不存在: {table_name}.{pk_name}={pk_value}.{sk_name}={sk_value}")
                return
            
            # 更新数据
            record = self._tables[table_name][pk_value][sk_value]
            record.update(update_data)
            
            print(f"成功更新 {table_name}: {pk_name}={pk_value}, {sk_name}={sk_value}, 数据: {update_data}")
    
    def get(self, table_name: str, primary_key: Tuple[str, str], 
            secondary_key: Tuple[str, str]) -> Optional[Dict[str, Any]]:
        """
        获取指定记录
        
        Args:
            table_name: 表名
            primary_key: 主键，格式为 (键名, 键值)
            secondary_key: 副键，格式为 (键名, 键值)
            
        Returns:
            记录数据字典，如果不存在则返回 None
        """
        with self._lock:
            pk_name, pk_value = primary_key
            sk_name, sk_value = secondary_key
            
            if (table_name in self._tables and 
                pk_value in self._tables[table_name] and
                sk_value in self._tables[table_name][pk_value]):
                return copy.deepcopy(self._tables[table_name][pk_value][sk_value])
            
            return None
    
    def delete(self, table_name: str, primary_key: Tuple[str, str], 
               secondary_key: Tuple[str, str]) -> bool:
        """
        删除指定记录
        
        Args:
            table_name: 表名
            primary_key: 主键，格式为 (键名, 键值)
            secondary_key: 副键，格式为 (键名, 键值)
            
        Returns:
            删除成功返回 True，记录不存在返回 False
        """
        with self._lock:
            pk_name, pk_value = primary_key
            sk_name, sk_value = secondary_key
            
            if (table_name in self._tables and 
                pk_value in self._tables[table_name] and
                sk_value in self._tables[table_name][pk_value]):
                del self._tables[table_name][pk_value][sk_value]
                print(f"成功删除 {table_name}: {pk_name}={pk_value}, {sk_name}={sk_value}")
                return True
            
            print(f"警告: 记录不存在，无法删除: {table_name}.{pk_name}={pk_value}.{sk_name}={sk_value}")
            return False
    
    def list_all_data(self) -> Dict[str, Any]:
        """列出所有数据（用于调试）"""
        with self._lock:
            return copy.deepcopy(self._tables)
    
    def clear_table(self, table_name: str) -> None:
        """清空指定表的数据"""
        with self._lock:
            if table_name in self._tables:
                self._tables[table_name] = {}
                print(f"已清空表 {table_name}")
    
    def to_json(self) -> str:
        """将所有数据转换为 JSON 字符串（用于持久化或调试）"""
        with self._lock:
            return json.dumps(self._tables, indent=2, ensure_ascii=False)

    # ============ 便于跨 Pod 传递的快照能力 ============
    def snapshot(self) -> Dict[str, Any]:
        """返回当前内存状态的深拷贝，可安全进行 JSON 序列化/通过 XCom 传递。"""
        return copy.deepcopy(self._tables)

    @classmethod
    def from_snapshot(cls, data: Optional[Dict[str, Any]]):
        """基于传入的字典状态还原一个新的客户端实例。"""
        client = cls()
        if data:
            client._tables = copy.deepcopy(data)
        return client


class NoSQLModule:
    """模拟 nosql 模块，提供单例模式的数据库客户端"""
    
    _instance: Optional[NoSQLClient] = None
    _lock = threading.Lock()
    
    @classmethod
    def get_instance(cls) -> NoSQLClient:
        """获取 NoSQL 客户端的单例实例"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = NoSQLClient()
                    print("NoSQL 客户端实例已创建")
        return cls._instance
    
    @classmethod
    def reset_instance(cls) -> None:
        """重置单例实例（主要用于测试）"""
        with cls._lock:
            cls._instance = None
            print("NoSQL 客户端实例已重置")


# 创建模块级别的 nosql 对象，模拟原始的调用方式
# 这样业务代码就可以用 nosql.nosql.get_instance() 来调用了
nosql = NoSQLModule()

# 为了确保完全兼容，再创建一个 nosql 属性指向自己
# 这样支持 from . import nosql; nosql.nosql.get_instance() 的调用方式
setattr(nosql, 'nosql', nosql)


# 为了方便调试，提供一些辅助函数
def print_all_data():
    """打印所有存储的数据"""
    client = nosql.get_instance()
    data = client.list_all_data()
    print("当前数据库中的所有数据:")
    print(json.dumps(data, indent=2, ensure_ascii=False))


def clear_all_data():
    """清空所有数据"""
    nosql.reset_instance()
    print("所有数据已清空")


if __name__ == "__main__":
    # 测试代码
    print("开始测试 NoSQL 模拟实现...")
    
    client = nosql.get_instance()
    
    # 测试插入
    client.insert("flights", ("trip_id", "trip_123"), ("flight_id", "flight_456"), 
                  {"price": "1000", "status": "pending"})
    
    # 测试更新
    client.update("flights", ("trip_id", "trip_123"), ("flight_id", "flight_456"), 
                  {"status": "booked"})
    
    # 测试获取
    record = client.get("flights", ("trip_id", "trip_123"), ("flight_id", "flight_456"))
    print(f"获取到的记录: {record}")
    
    # 打印所有数据
    print_all_data()

# =============================================================
# 下方提供一组“纯字典”API，便于在业务函数中只多传一个参数（状态字典），
# 而无需依赖进程内单例，从而在 Knative 的多 Pod 环境下保持一致。

NoSQLState = Dict[str, Dict[str, Dict[str, Dict[str, Any]]]]

def init_state() -> NoSQLState:
    """创建一个全新的、可序列化的 NoSQL 状态字典。"""
    return {}

def _ensure_slots(state: NoSQLState, table_name: str, pk_value: str) -> Dict[str, Dict[str, Any]]:
    table_map = state.setdefault(table_name, {})
    return table_map.setdefault(pk_value, {})

def insert(state: NoSQLState, table_name: str,
           primary_key: Tuple[str, str], secondary_key: Tuple[str, str],
           data: Dict[str, Any]) -> None:
    """在传入的状态字典上执行插入操作（原地生效）。"""
    pk_name, pk_value = primary_key
    sk_name, sk_value = secondary_key
    slot = _ensure_slots(state, table_name, pk_value)
    record = copy.deepcopy(data)
    record[pk_name] = pk_value
    record[sk_name] = sk_value
    slot[sk_value] = record

def get(state: NoSQLState, table_name: str,
        primary_key: Tuple[str, str], secondary_key: Tuple[str, str]) -> Optional[Dict[str, Any]]:
    """在传入的状态字典上读取记录。"""
    pk_value = primary_key[1]
    sk_value = secondary_key[1]
    return state.get(table_name, {}).get(pk_value, {}).get(sk_value)

def update(state: NoSQLState, table_name: str,
           primary_key: Tuple[str, str], secondary_key: Tuple[str, str],
           patch: Dict[str, Any]) -> None:
    """在传入的状态字典上更新记录（存在则原地更新）。"""
    rec = get(state, table_name, primary_key, secondary_key)
    if rec is not None:
        rec.update(patch)

def delete(state: NoSQLState, table_name: str,
           primary_key: Tuple[str, str], secondary_key: Tuple[str, str]) -> bool:
    """在传入的状态字典上删除记录。"""
    pk_value = primary_key[1]
    sk_value = secondary_key[1]
    if table_name in state and pk_value in state[table_name] and sk_value in state[table_name][pk_value]:
        del state[table_name][pk_value][sk_value]
        return True
    return False