import pendulum
from airflow.decorators import dag, task
import logging
from functools import wraps
from time import time
import time as t_module
import uuid
import copy
from typing import Dict, Any, Tuple, Optional

# NoSQL数据库模拟函数
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

# trip_input相关函数
def allocate_nosql() -> dict:
    return {
        "flights": {
            "primary_key": "trip_id",
            "secondary_key": "flight_id"
        },
        "car_rentals": {
            "primary_key": "trip_id",
            "secondary_key": "rental_id"
        },
        "hotel_booking": {
            "primary_key": "trip_id",
            "secondary_key": "booking_id"
        }
    }

def generate_input(
    data_dir, size, benchmarks_bucket, input_buckets, output_buckets, upload_func, nosql_func
):
    input_config = {}

    # test - invoke a single trip, succeed
    # small - fail in the middle
    # large - fail at the last step

    trip_details = {
        "flight_depart": "ZRH",
        "flight_arrive": "KTW",
        "flight_date": "2020-08-22T13:00:00",
        "hotel_stars": "3",
        "hotel_nights": "3",
        "hotel_distance": "1500",
        "hotel_price_max": "150",
        "rental_class": "compact",
        "rental_price_max": "100",
        "rental_duration": 3,
        "rental_requests": ["full_tank", "CDW", "assistance"]
    }

    size_results = {
        "test": {"result": "success"},
        "small": {"result": "failure", "reason": "hotel"},
        "large": {"result": "failure", "reason": "confirm"}
    }
    trip_details["expected_result"] = size_results[size]

    return trip_details

#########################################图部分########################################################
# by Jonathan Prieto-Cubides https://stackoverflow.com/questions/1622943/timeit-versus-timing-decorator
def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        logging.info('func:%r args:[%r, %r] took: %f sec. Start: %f, End: %f' % (f.__name__, args, kw, te-ts, ts, te))
        return result
    return wrap

@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False)
def dag_w1_d7():
    @task
    @timing
    def func_1_1():
        # 1. 合并 func_1_1 的逻辑，生成初始输入
        logging.info("======= vertex1 execution (integrated) =======")
        event = generate_input(
            data_dir=None,
            size="test",
            benchmarks_bucket=None,
            input_buckets=None,
            output_buckets=None,
            upload_func=None,
            nosql_func=None
        )
        event["request-id"] = str(uuid.uuid4())
        db_state = init_state()

        # 2. 执行原来的业务逻辑
        logging.info("======= vertex2 execution =======")
        
        # 内联 reserve_hotel.handler(db_state, event) 的逻辑
        # nosql_client = nosql.nosql.get_instance() # %%% 去掉全局客户端 %%%
        # %%% 注释掉全局单例客户端，改为使用无状态的函数式API，便于在Airflow任务间传递状态 %%%
        nosql_table_name = "hotel_booking"

        expected_result = event["expected_result"]
        if expected_result["result"] == "failure" and expected_result["reason"] == "hotel":
            raise RuntimeError("Failed to book the hotel!")

        # We start with the hotel
        trip_id = str(uuid.uuid4().hex)
        hotel_booking_id = event["request-id"]

        # Simulate return from a service
        hotel_price = "130"
        hotel_name = "BestEver Hotel"

        # %%% 调用无状态的 insert 函数，传入 db_state 并接收返回的新 state %%%
        # %%% 改用nosql模块的insert函数替代客户端方法，第一个参数传入db_state并接收返回值 %%%
        new_db_state = insert(
            db_state, # %%% 传入当前 state %%%
            # %%% db_state作为第一个参数传入，确保函数操作的是当前状态 %%%
            nosql_table_name,
            ("trip_id", trip_id),
            ("booking_id", hotel_booking_id),
            {
                **{key: event[key] for key in event.keys() if key.startswith("hotel_")},
                "hotel_price": hotel_price,
                "hotel_name": hotel_name,
                "status": "pending",
            },
        )

        # %%% 返回 event 和新的 db_state %%%
        new_event = {"trip_id": trip_id, "booking_id": hotel_booking_id, **event}
        # %%% 返回值改为元组，包含更新后的db_state和event，供下游任务使用 %%%
        
        t_module.sleep(125 / 1000)
        return new_db_state, new_event

    @task
    @timing
    def func_1_2(upstream_output):
        db_state, event = upstream_output
        logging.info("======= vertex3 execution =======")
        
        # 内联 reserve_rental.handler(db_state, event) 的逻辑
        # nosql_client = nosql.nosql.get_instance() # %%% 去掉全局客户端 %%%
        # %%% 注释掉全局单例客户端，改为使用无状态的函数式API，便于在Airflow任务间传递状态 %%%
        nosql_table_name = "car_rentals"

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
        new_db_state = insert(
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

        new_event = {"trip_id": trip_id, "rental_id": rental_id, **event}
        # %%% 返回值改为元组，包含更新后的db_state和event，供下游任务使用 %%%
        
        t_module.sleep(125 / 1000)
        return new_db_state, new_event
    
    @task
    @timing
    def func_1_3(upstream_output):
        db_state, event = upstream_output
        logging.info("======= vertex4 execution =======")
        
        # 内联 reserve_flight.handler(db_state, event) 的逻辑
        # nosql_client = nosql.nosql.get_instance() # %%% 去掉全局客户端 %%%
        # %%% 注释掉全局单例客户端，改为使用无状态的函数式API，便于在Airflow任务间传递状态 %%%
        nosql_table_name = "flights"

        expected_result = event["expected_result"]
        if expected_result["result"] == "failure" and expected_result["reason"] == "flight":
            raise RuntimeError("Failed to book a flight!")

        # We start with the hotel
        trip_id = event["trip_id"]
        flight_id = event["request-id"]

        # Simulate return from a service
        flight_price = "1000"
        flight_connections = ["WAW"]
        flight_duration = "4h30m"

        # %%% 改用nosql模块的insert函数替代客户端方法，第一个参数传入db_state并接收返回值 %%%
        new_db_state = insert(
            db_state, # %%% db_state作为第一个参数传入，确保函数操作的是当前状态 %%%
            nosql_table_name,
            ("trip_id", trip_id),
            ("flight_id", flight_id),
            {
                **{key: event[key] for key in event.keys() if key.startswith("flight_")},
                "price": flight_price,
                "connections": flight_connections,
                "duration": flight_duration,
                "status": "pending",
            },
        )

        new_event = {
            "trip_id": trip_id,
            "flight_id": flight_id,
            **{key: event[key] for key in ["booking_id", "rental_id"]},
            "expected_result": expected_result,
        }
        # %%% 返回值改为元组，包含更新后的db_state和event，供下游任务使用 %%%
        
        t_module.sleep(125 / 1000)
        return new_db_state, new_event
    
    @task
    @timing
    def func_1_4(upstream_output):
        db_state, event = upstream_output
        logging.info("======= vertex5 execution =======")
        
        # 内联 confirm.handler(db_state, event) 的逻辑
        # nosql_client = nosql.nosql.get_instance() # %%% 去掉全局客户端 %%%
        # %%% 注释掉全局单例客户端，改为使用无状态的函数式API，便于在Airflow任务间传递状态 %%%

        expected_result = event["expected_result"]
        if expected_result["result"] == "failure" and expected_result["reason"] == "confirm":
            raise RuntimeError("Failed to confirm the booking!")

        trip_id = event["trip_id"]

        # Confirm flight
        # %%% 将上一步的 state 传入，并接收返回的新 state %%%
        # %%% 改用nosql模块的update函数替代客户端方法，支持链式状态传递 %%%
        nosql_table_name = "flights"
        flight_id = event["flight_id"]
        db_state_after_flight = update(
            db_state, # %%% 传入当前 state %%%
            nosql_table_name,
            ("trip_id", trip_id),
            ("flight_id", flight_id),
            {"status": "booked"},
        )

        # Confirm car rental
        # %%% 使用上一步返回的状态，实现连续更新操作的状态链式传递 %%%
        nosql_table_name = "car_rentals"
        db_state_after_rental = update(
            db_state_after_flight, # %%% 传入上一步更新后的 state %%%
            nosql_table_name,
            ("trip_id", trip_id),
            ("rental_id", event["rental_id"]),
            {"status": "booked"},
        )

        # Confirm hotel booking
        # %%% 继续链式传递状态，确保所有更新操作在同一个状态基础上进行 %%%
        nosql_table_name = "hotel_booking"
        db_state_after_hotel = update(
            db_state_after_rental, # %%% 传入上一步更新后的 state %%%
            nosql_table_name,
            ("trip_id", trip_id),
            ("booking_id", event["booking_id"]),
            {"status": "booked"},
        )

        # %%% 返回最终的 state 和更新后的 event %%%
        event['status'] = 'success'
        new_event = event
        # %%% 返回值改为元组，包含最终更新后的db_state和event，供下游任务使用 %%%
        
        t_module.sleep(125 / 1000)
        return db_state_after_hotel, new_event

    @task
    @timing
    def func_1_5(upstream_output):
        db_state, event = upstream_output
        logging.info("======= vertex6 execution =======")
        
        # 内联 cancel_flight.handler(db_state, event) 的逻辑
        # nosql_client = nosql.nosql.get_instance() # %%% 去掉全局客户端 %%%
        # %%% 注释掉全局单例客户端，改为使用无状态的函数式API，便于在Airflow任务间传递状态 %%%

        trip_id = event["trip_id"]

        # Confirm flight
        nosql_table_name = "flights"
        flight_id = event["flight_id"]
        # %%% 改用nosql模块的delete函数替代客户端方法，第一个参数传入db_state并接收返回值 %%%
        new_db_state = delete(db_state, nosql_table_name, ("trip_id", trip_id), ("flight_id", flight_id))

        event.pop("flight_id")
        new_event = event
        # %%% 返回值改为元组，包含更新后的db_state和event，供下游任务使用 %%%
        
        t_module.sleep(125 / 1000)
        return new_db_state, new_event

    @task
    @timing
    def func_1_6(upstream_output):
        db_state, event = upstream_output
        logging.info("======= vertex7 execution =======")
        
        # 内联 cancel_rental.handler(db_state, event) 的逻辑
        # nosql_client = nosql.nosql.get_instance() # %%% 去掉全局客户端 %%%
        # %%% 注释掉全局单例客户端，改为使用无状态的函数式API，便于在Airflow任务间传递状态 %%%

        trip_id = event["trip_id"]

        # Confirm flight
        nosql_table_name = "car_rentals"
        rental_id = event["rental_id"]
        # %%% 改用nosql模块的delete函数替代客户端方法，第一个参数传入db_state并接收返回值 %%%
        new_db_state = delete(db_state, nosql_table_name, ("trip_id", trip_id), ("rental_id", rental_id))

        event.pop("rental_id")
        new_event = event
        # %%% 返回值改为元组，包含更新后的db_state和event，供下游任务使用 %%%
        
        t_module.sleep(125 / 1000)
        return new_db_state, new_event

    @task
    @timing
    def func_1_7(upstream_output):
        db_state, event = upstream_output
        
        # 内联 cancel_hotel.handler(db_state, event) 的逻辑
        # nosql_client = nosql.nosql.get_instance() # %%% 去掉全局客户端 %%%
        # %%% 注释掉全局单例客户端，改为使用无状态的函数式API，便于在Airflow任务间传递状态 %%%

        trip_id = event["trip_id"]

        # Confirm flight
        nosql_table_name = "hotel_booking"
        booking_id = event["booking_id"]
        # %%% 改用nosql模块的delete函数替代客户端方法，第一个参数传入db_state并接收返回值 %%%
        new_db_state = delete(db_state, nosql_table_name, ("trip_id", trip_id), ("booking_id", booking_id))

        new_event = {"trip_id": trip_id, "status": "failure"}
        # %%% 返回值改为元组，包含更新后的db_state和event，供下游任务使用 %%%
        
        logging.info("======= vertex8 execution =======")
        t_module.sleep(125 / 1000)
        return new_db_state, new_event

    # specify data flow
    func_1_1_output = func_1_1()
    func_1_2_output = func_1_2(func_1_1_output)
    func_1_3_output = func_1_3(func_1_2_output)
    func_1_4_output = func_1_4(func_1_3_output)
    func_1_5_output = func_1_5(func_1_4_output)
    func_1_6_output = func_1_6(func_1_5_output)
    func_1_7(func_1_6_output)

# execute dag
etl_dag = dag_w1_d7()