from . import nosql

# nosql_client = nosql.nosql.get_instance() # 去掉全局客户端


def handler(db_state, event): # 接收 db_state

    trip_id = event["trip_id"]

    # Confirm flight
    nosql_table_name = "hotel_booking"
    booking_id = event["booking_id"]
    new_db_state = nosql.delete(db_state, nosql_table_name, ("trip_id", trip_id), ("booking_id", booking_id))

    return new_db_state, {"trip_id": trip_id, "status": "failure"}
