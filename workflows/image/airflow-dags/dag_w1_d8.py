import pendulum
from airflow.decorators import dag, task
import logging
from functools import wraps
from time import time
import time as t_module
import uuid

# 添加trip-booking模块到路径
import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
trip_booking_path = os.path.join(current_dir, "6200.trip-booking")
sys.path.append(trip_booking_path)

# 导入trip-booking模块
from python import cancel_flight, cancel_rental, cancel_hotel
from python import confirm, reserve_flight, reserve_hotel, reserve_rental
from python import nosql as nd
import trip_input

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
def dag_w1_d8():
    @task
    @timing
    def func_1_1():
        logging.info("======= vertex1 execution =======")
        eventnow = trip_input.generate_input(
            data_dir=None,
            size="test",
            benchmarks_bucket=None,
            input_buckets=None,
            output_buckets=None,
            upload_func=None,
            nosql_func=None
        )
        # 添加必需的 request-id 字段
        eventnow["request-id"] = str(uuid.uuid4())
        # 初始化可序列化的字典态 nosql 状态
        db_state = nd.init_state()
        t_module.sleep(125 / 1000)
        return eventnow, db_state

    @task
    @timing
    def func_1_2(upstream_output):
        event, db_state = upstream_output
        logging.info("======= vertex2 execution =======")
        eventnow, db_state = reserve_hotel.handler(event, db_state)
        t_module.sleep(125 / 1000)
        return eventnow, db_state

    @task
    @timing
    def func_1_3(upstream_output):
        event, db_state = upstream_output
        logging.info("======= vertex3 execution =======")
        eventnow, db_state = reserve_rental.handler(event, db_state)
        t_module.sleep(125 / 1000)
        return eventnow, db_state
    
    @task
    @timing
    def func_1_4(upstream_output):
        event, db_state = upstream_output
        logging.info("======= vertex4 execution =======")
        eventnow, db_state = reserve_flight.handler(event, db_state)
        t_module.sleep(125 / 1000)
        return eventnow, db_state
    
    @task
    @timing
    def func_1_5(upstream_output):
        event, db_state = upstream_output
        logging.info("======= vertex5 execution =======")
        eventnow, db_state = confirm.handler(event, db_state)
        t_module.sleep(125 / 1000)
        return eventnow, db_state

    @task
    @timing
    def func_1_6(upstream_output):
        event, db_state = upstream_output
        logging.info("======= vertex6 execution =======")
        eventnow, db_state = cancel_flight.handler(event, db_state)
        t_module.sleep(125 / 1000)
        return eventnow, db_state

    @task
    @timing
    def func_1_7(upstream_output):
        event, db_state = upstream_output
        logging.info("======= vertex7 execution =======")
        eventnow, db_state = cancel_rental.handler(event, db_state)
        t_module.sleep(125 / 1000)
        return eventnow, db_state

    @task
    @timing
    def func_1_8(upstream_output):
        event, db_state = upstream_output
        eventnow, db_state = cancel_hotel.handler(event, db_state)
        logging.info("======= vertex8 execution =======")
        t_module.sleep(125 / 1000)
        return eventnow, db_state

    # specify data flow
# specify data flow
    func_1_1_output = func_1_1()
    func_1_2_output = func_1_2(func_1_1_output)
    func_1_3_output = func_1_3(func_1_2_output)
    func_1_4_output = func_1_4(func_1_3_output)
    func_1_5_output = func_1_5(func_1_4_output)
    func_1_6_output = func_1_6(func_1_5_output)
    func_1_7_output = func_1_7(func_1_6_output)
    func_1_8(func_1_7_output)

# execute dag
etl_dag = dag_w1_d8()
