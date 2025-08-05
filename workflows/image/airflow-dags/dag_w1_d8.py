import pendulum
from airflow.decorators import dag, task
import logging
from functools import wraps
from time import time
import time as t_module
import sys
import os

# 将带~的路径转换为绝对路径
module_path = os.path.expanduser("~/my_airflow/6200.trip-booking")  # ~被替换为家目录
sys.path.append(module_path)  # 添加处理后的路径
import cancel_flight, cancel_rental, cancel_hotel
import confirm, reserve_flight, reserve_hotel, reserve_rental
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
        eventnow = trip_input.generate_input(size = large)
        t_module.sleep(125 / 1000)
        return eventnow

    @task
    @timing
    def func_1_2(event):
        logging.info("======= vertex2 execution =======")
        eventnow = reserve_hotel.handler(event)
        t_module.sleep(125 / 1000)
        return eventnow

    @task
    @timing
    def func_1_3(even):
        logging.info("======= vertex3 execution =======")
        eventnow = reserve_rental.handler(event)
        t_module.sleep(125 / 1000)
        return eventnow
    
    @task
    @timing
    def func_1_4(event):
        logging.info("======= vertex4 execution =======")
        eventnow = reserve_flight.handler(event)
        t_module.sleep(125 / 1000)
        return eventnow
    
    @task
    @timing
    def func_1_5(event):
        logging.info("======= vertex5 execution =======")
        eventnow = confirm.handler(event)
        t_module.sleep(125 / 1000)
        return eventnow

    @task
    @timing
    def func_1_6(event):
        logging.info("======= vertex6 execution =======")
        eventnow = cancel_flight.handler(event)
        t_module.sleep(125 / 1000)
        return eventnow

    @task
    @timing
    def func_1_7(event):
        logging.info("======= vertex7 execution =======")
        eventnow = cancel_rental.handler(event)
        t_module.sleep(125 / 1000)
        return eventnow

    @task
    @timing
    def func_1_8(event):
        eventnow = cancel_hotel.handler(event)
        logging.info("======= vertex8 execution =======")
        t_module.sleep(125 / 1000)
        return eventnow

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
