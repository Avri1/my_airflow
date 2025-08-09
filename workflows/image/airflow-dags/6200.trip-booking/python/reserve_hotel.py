import uuid

from . import nosql

# nosql_client = nosql.nosql.get_instance() # 去掉全局客户端
nosql_table_name = "hotel_booking"


def handler(db_state, event): # 接收 db_state 作为参数

    expected_result = event["expected_result"]
    if expected_result["result"] == "failure" and expected_result["reason"] == "hotel":
        raise RuntimeError("Failed to book the hotel!")

    # We start with the hotel
    trip_id = str(uuid.uuid4().hex)
    hotel_booking_id = event["request-id"]

    # Simulate return from a service
    hotel_price = "130"
    hotel_name = "BestEver Hotel"

    # 调用无状态的 insert 函数，传入 db_state 并接收返回的新 state
    new_db_state = nosql.insert(
        db_state, # 传入当前 state
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

    # 返回 event 和新的 db_state
    return new_db_state, {"trip_id": trip_id, "booking_id": hotel_booking_id, **event}
