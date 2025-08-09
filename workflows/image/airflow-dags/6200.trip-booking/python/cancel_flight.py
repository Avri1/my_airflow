from . import nosql

# nosql_client = nosql.nosql.get_instance() # 去掉全局客户端


def handler(db_state, event): # 接收 db_state

    trip_id = event["trip_id"]

    # Confirm flight
    nosql_table_name = "flights"
    flight_id = event["flight_id"]
    new_db_state = nosql.delete(db_state, nosql_table_name, ("trip_id", trip_id), ("flight_id", flight_id))

    event.pop("flight_id")
    return new_db_state, event
