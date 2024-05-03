import datetime
import os
from io import StringIO

import pandas as pd
import requests
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


def get_events_df():
    # URL of the CSV data
    csv_url = "https://tableau.admin.uillinois.edu/views/DailyEventSummary/DailyEvents/736dba17-6e8f-4ccf-af5d-fd884dfd32ce/dc632d93-ace1-4bac-b605-143007524566.csv"
    # csv_url = "https://tableau.admin.uillinois.edu/views/DailyEventSummary/DailyEvents/5f5199d9-b39c-405f-84ef-1c6088e8abe3/6278da8a-42bb-41c1-98ee-aff940137635.csv"

    # Fetch CSV data from the URL
    response = requests.get(csv_url, timeout=10)
    csv_data = response.text
    print("  ✅ Got data from Tableau")

    # Read CSV into a DataFrame
    df = pd.read_csv(StringIO(csv_data))

    # Fix EndTime column, delete old one
    df["end_time"] = pd.to_datetime(
        df["EndTime"],
        format="%m/%d/%Y %I:%M:%S %p",
    )
    df = df.drop("EndTime", axis=1)

    # Drop unnecessary cols
    df = df.drop("Measure Values", axis=1)
    df = df.drop("Open/Close", axis=1)

    # Create the 'start_time' attribute by combining 'StartDate' and 'StartTime'
    df["start_time"] = pd.to_datetime(
        df["StartDate"] + " " + df["StartTime"].map(lambda s: s.split(" ", 1)[1]),
        format="%m/%d/%Y %I:%M:%S %p",
    ).dt.tz_localize("America/Chicago", ambiguous=[True] * len(df))

    df["end_time"] = pd.to_datetime(
        df["end_time"], format="%Y-%m-%d %H:%M:%S"
    ).dt.tz_localize("America/Chicago", ambiguous=[True] * len(df))

    # Remove 'StartDate' and 'StartTime' column as they're no longer needed
    df = df.drop(columns=["StartDate", "StartTime"])

    # Rename uppercase columns
    df = df.rename(
        columns={
            "Building": "building",
            "Customer": "customer",
            "CustomerContact": "customer_contact",
            "EventName": "event_name",
            "Room": "room",
        }
    )

    print("  ✅ Finished processing data")

    return df


def convert_df_to_mongo_format(df):
    # Convert DataFrame to a list of dictionaries (documents)
    data_dict = df.to_dict(orient="records")

    # Each building name corresponds to a list of events in that building (key: building, value: list[event])
    building_dict = {}
    for event in data_dict:
        this_building = event["building"]
        if this_building not in building_dict:
            building_dict[this_building] = []

        building_dict[this_building].append(event)

    # Convert dict to list; each item has building and events
    buildings_with_events = []
    for pair in building_dict.items():
        building = pair[0]
        events = pair[1]
        buildings_with_events.append({"building": building, "events": events})

    return buildings_with_events


def update_database(events_df, buildings_with_events):
    uri = os.environ.get("MONGODB_URI")

    # Create a new client and connect to the server
    print("  🌐 Connecting to Mongo...")
    client = MongoClient(uri, server_api=ServerApi("1"))
    print("  ✅ Connected to Mongo")

    illinois_events = client["IllinoisEvents"]
    events_collection = illinois_events["Events"]

    # Remove all events from collection
    print("  🗑️  Deleting old events...")
    events_collection.delete_many({})
    print("  ✅️ Deleted old events")

    # Insert data into the empty collection
    print("  🎁 Inserting new events...")
    events_collection.insert_many(buildings_with_events)
    print("  ✅️ Inserted new events")

    # Keep track of updated time
    print("  ⏰ Adding update_time...")
    illinois_events["UpdateTimes"].insert_one(
        {
            "update_time": datetime.datetime.utcnow(),
            "events": len(events_df),
            "buildings": len(buildings_with_events),
        }
    )
    print("  ✅️ Added update time")

    # Close the MongoDB Atlas client
    client.close()


def lambda_handler(request_event, context):
    print("Step 1 : Process data from Tableau dashboard")
    events = get_events_df()
    print("✅ Finished Step 1")
    print()

    print("Step 2 : Reorganize data for MongoDB")
    buildings_with_events = convert_df_to_mongo_format(events)
    print("✅ Finished Step 2")
    print()

    print("Step 3 : Update database")
    update_database(events, buildings_with_events)
    print("✅ Finished Step 3")
    print()

    print("Job complete! 🎉")

    return "Updated data"


# lambda_handler(None, None)
