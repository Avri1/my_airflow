from . import nosql

# nosql_client = nosql.nosql.get_instance() # %%% 去掉全局客户端 %%%
# %%% 注释掉全局单例客户端，改为使用无状态的函数式API，便于在Airflow任务间传递状态 %%%
nosql_table_name = "car_rentals"


def handler(db_state, event): # %%% 接收 db_state %%%
    # %%% 函数签名增加db_state参数，用于接收上游任务传递的数据库状态字典 %%%

    expected_result = event["expected_result"]
    if expected_result["result"] == "failure" and expected_result["reason"] == "rental":
        raise RuntimeError("Failed to rent a car!")

    # We start with the hotel
    trip_id = event["trip_id"]
    rental_id = event["request-id"]

    # Simulate return from a service
    car_price = "125"
    car_name = "Fiat 126P"

    # %%% 改用nosql模块的insert函数替代客户端方法，第一个参数传入db_state并接收返回值 %%%
    new_db_state = nosql.insert(
        db_state, # %%% db_state作为第一个参数传入，确保函数操作的是当前状态 %%%
        nosql_table_name,
        ("trip_id", trip_id),
        ("rental_id", rental_id),
        {
            **{key: event[key] for key in event.keys() if key.startswith("rental_")},
            "rental_price": car_price,
            "rental_name": car_name,
            "status": "pending",
        },
    )

    return new_db_state, {"trip_id": trip_id, "rental_id": rental_id, **event}
    # %%% 返回值改为元组，包含更新后的db_state和event，供下游任务使用 %%%
