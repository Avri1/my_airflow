from . import nosql as nd

nosql_table_name = "flights"


def handler(event, db_state):

    trip_id = event["trip_id"]
    flight_id = event["flight_id"]

    nd.update(
        db_state,
        nosql_table_name,
        ("trip_id", trip_id),
        ("flight_id", flight_id),
        {"status": "cancelled"},
    )

    return {"trip_id": trip_id, "flight_id": flight_id, "status": "cancelled"}, db_state

from . import nosql

nosql_client = nosql.nosql.get_instance()


def handler(event):

    trip_id = event["trip_id"]

    # Confirm flight
    nosql_table_name = "flights"
    flight_id = event["flight_id"]
    nosql_client.delete(nosql_table_name, ("trip_id", trip_id), ("flight_id", flight_id))

    event.pop("flight_id")
    return event
