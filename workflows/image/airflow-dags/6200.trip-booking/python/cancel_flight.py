from . import nosql

# nosql_client = nosql.nosql.get_instance() # %%% 去掉全局客户端 %%%
# %%% 注释掉全局单例客户端，改为使用无状态的函数式API，便于在Airflow任务间传递状态 %%%


def handler(db_state, event): # %%% 接收 db_state %%%
    # %%% 函数签名增加db_state参数，用于接收上游任务传递的数据库状态字典 %%%

    trip_id = event["trip_id"]

    # Confirm flight
    nosql_table_name = "flights"
    flight_id = event["flight_id"]
    # %%% 改用nosql模块的delete函数替代客户端方法，第一个参数传入db_state并接收返回值 %%%
    new_db_state = nosql.delete(db_state, nosql_table_name, ("trip_id", trip_id), ("flight_id", flight_id))

    event.pop("flight_id")
    return new_db_state, event
    # %%% 返回值改为元组，包含更新后的db_state和event，供下游任务使用 %%%
