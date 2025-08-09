"""
一个简化的 NoSQL 数据库模拟实现。
所有函数都直接操作传入的 Python 字典，使其成为无状态的工具模块。
"""
import copy
from typing import Dict, Any, Tuple, Optional

def init_state() -> Dict[str, Any]:
    """创建一个空的数据库状态字典"""
    return {}

def insert(db_state: Dict[str, Any], table_name: str, primary_key: Tuple[str, str],
           secondary_key: Tuple[str, str], data: Dict[str, Any]) -> Dict[str, Any]:
    """
    向数据库状态中插入一条新记录。
    返回一个新的、更新后的数据库状态字典。
    """
    # 使用深拷贝以避免修改原始字典，确保函数无副作用
    new_state = copy.deepcopy(db_state)

    # 确保表存在
    if table_name not in new_state:
        new_state[table_name] = {}

    # 确保主键存在
    pk_name, pk_value = primary_key
    if pk_value not in new_state[table_name]:
        new_state[table_name][pk_value] = {}

    # 插入数据
    sk_name, sk_value = secondary_key
    record = copy.deepcopy(data)
    record[pk_name] = pk_value
    record[sk_name] = sk_value

    new_state[table_name][pk_value][sk_value] = record
    print(f"成功插入到 {table_name}: {pk_name}={pk_value}, {sk_name}={sk_value}")

    return new_state

def update(db_state: Dict[str, Any], table_name: str, primary_key: Tuple[str, str],
           secondary_key: Tuple[str, str], update_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    更新数据库状态中的一条现有记录。
    返回一个新的、更新后的数据库状态字典。
    """
    new_state = copy.deepcopy(db_state)

    pk_name, pk_value = primary_key
    sk_name, sk_value = secondary_key

    # 检查记录是否存在
    if (table_name not in new_state or
        pk_value not in new_state[table_name] or
        sk_value not in new_state[table_name][pk_value]):
        print(f"警告: 记录不存在，无法更新: {table_name}.{pk_name}={pk_value}.{sk_name}={sk_value}")
        return new_state # 返回原始状态的拷贝

    # 更新数据
    record = new_state[table_name][pk_value][sk_value]
    record.update(update_data)
    print(f"成功更新 {table_name}: {pk_name}={pk_value}, {sk_name}={sk_value}, 数据: {update_data}")

    return new_state

def get(db_state: Dict[str, Any], table_name: str, primary_key: Tuple[str, str],
        secondary_key: Tuple[str, str]) -> Optional[Dict[str, Any]]:
    """从数据库状态中获取一条记录，不修改状态。"""
    pk_name, pk_value = primary_key
    sk_name, sk_value = secondary_key

    if (table_name in db_state and
        pk_value in db_state[table_name] and
        sk_value in db_state[table_name][pk_value]):
        # 返回记录的深拷贝以防止外部修改
        return copy.deepcopy(db_state[table_name][pk_value][sk_value])

    return None

def delete(db_state: Dict[str, Any], table_name: str, primary_key: Tuple[str, str],
           secondary_key: Tuple[str, str]) -> Dict[str, Any]:
    """
    从数据库状态中删除一条记录。
    返回一个新的、更新后的数据库状态字典。
    """
    new_state = copy.deepcopy(db_state)
    pk_name, pk_value = primary_key
    sk_name, sk_value = secondary_key

    if (table_name in new_state and
        pk_value in new_state[table_name] and
        sk_value in new_state[table_name][pk_value]):
        del new_state[table_name][pk_value][sk_value]
        print(f"成功删除 {table_name}: {pk_name}={pk_value}, {sk_name}={sk_value}")
    else:
        print(f"警告: 记录不存在，无法删除: {table_name}.{pk_name}={pk_value}.{sk_name}={sk_value}")

    return new_state
