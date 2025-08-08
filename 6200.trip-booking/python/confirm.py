from . import nosql as nd


def handler(event, db_state):

    expected_result = event["expected_result"]
    if expected_result["result"] == "failure" and expected_result["reason"] == "confirm":
        raise RuntimeError("Failed to confirm the booking!")

    trip_id = event["trip_id"]

    # Confirm flight
    nosql_table_name = "flights"
    flight_id = event["flight_id"]
    nd.update(
        db_state,
        nosql_table_name,
        ("trip_id", trip_id),
        ("flight_id", flight_id),
        {"status": "booked"},
    )

    # Confirm car rental
    nosql_table_name = "car_rentals"
    nd.update(
        db_state,
        nosql_table_name,
        ("trip_id", trip_id),
        ("rental_id", event["rental_id"]),
        {"status": "booked"},
    )

    # Confirm hotel booking
    nosql_table_name = "hotel_booking"
    nd.update(
        db_state,
        nosql_table_name,
        ("trip_id", trip_id),
        ("booking_id", event["booking_id"]),
        {"status": "booked"},
    )

    return {"trip_id": trip_id, "status": "success"}, db_state
