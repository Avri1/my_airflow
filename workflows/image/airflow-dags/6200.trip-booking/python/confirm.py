from . import nosql

# nosql_client = nosql.nosql.get_instance() # 去掉全局客户端


def handler(db_state, event): # 接收 db_state

    expected_result = event["expected_result"]
    if expected_result["result"] == "failure" and expected_result["reason"] == "confirm":
        raise RuntimeError("Failed to confirm the booking!")

    trip_id = event["trip_id"]

    # Confirm flight
    # 将上一步的 state 传入，并接收返回的新 state
    nosql_table_name = "flights"
    flight_id = event["flight_id"]
    db_state_after_flight = nosql.update(
        db_state, # 传入当前 state
        nosql_table_name,
        ("trip_id", trip_id),
        ("flight_id", flight_id),
        {"status": "booked"},
    )

    # Confirm car rental
    nosql_table_name = "car_rentals"
    db_state_after_rental = nosql.update(
        db_state_after_flight, # 传入上一步更新后的 state
        nosql_table_name,
        ("trip_id", trip_id),
        ("rental_id", event["rental_id"]),
        {"status": "booked"},
    )

    # Confirm hotel booking
    nosql_table_name = "hotel_booking"
    db_state_after_hotel = nosql.update(
        db_state_after_rental, # 传入上一步更新后的 state
        nosql_table_name,
        ("trip_id", trip_id),
        ("booking_id", event["booking_id"]),
        {"status": "booked"},
    )

    # 返回最终的 state 和 event
    return db_state_after_hotel, {"trip_id": trip_id, "status": "success"}
