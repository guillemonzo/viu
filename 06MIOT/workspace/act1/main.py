import pymongo
import pandas as pd
import numpy as np
import json as js


def csv_2_json(url):
    # Read file
    df = pd.read_csv(url, sep=",", encoding="utf-8")

    # Trim values
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    # Clean unknown values
    df = df.replace({'Antiguitat_carnet': 'Desconegut'}, np.nan)

    # Convert column to numeric value
    df['Antiguitat_carnet'] = pd.to_numeric(df['Antiguitat_carnet'])

    # Save as a json file
    df.to_json("./accidentes.json", orient="records")

    return df.to_json(orient="records")


def insert_to_mongo(db, records):
    # Get collection
    collection = db.collection

    # Convert from string to json object
    records = js.loads(records, encoding="utf-8")

    # Delete previous
    collection.delete_many({})

    # Insert documents
    collection.insert_many(records)


def get_accidents_grouped_by_day_and_time(db):
    # Get collection
    collection = db.collection

    data = collection.aggregate([
        {
            "$match": {
                "Antiguitat_carnet": {"$lte": 5}
            }
        },
        {
            "$group": {
                "_id": {
                   "weekDay": "$Descripcio_dia_setmana",
                   "time": "$Descripcio_torn"
                },
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {
               "count": -1
            }
        }
    ])

    print("Accidents grouped by day of the week and time:")
    for i in data:
        print(i)


def get_top_neighbourhoods_with_more_accidents(db):
    # Get collection
    collection = db.collection

    data = collection.aggregate([
        {
            "$group": {
                "_id": "$Nom_barri",
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {
                "count": -1
            }
        },
        {
            "$limit": 3
        }
    ])

    print("Top 3 neighbourhoods with more accidents:")
    for i in data:
        print(i)


def get_license_years_mean(db):
    # Get collection
    collection = db.collection

    data = collection.aggregate([
        {
            "$group": {
                "_id": None,
                "average": {"$avg": "$Antiguitat_carnet"}
            }
        }
    ])

    print("Mean of license years:")
    for i in data:
        print(i)


if __name__ == "__main__":
    # Data set url
    data_set_url = 'https://opendata-ajuntament.barcelona.cat/data/dataset/317e3743-fb79-4d2f-a128-5f12d2c9a55a/resource/6e2daeb5-e359-43ad-b0b5-7fdf438c8d6f/download/2018_accidents_vehicles_gu_bcn_.csv'

    # Convert to json
    json = csv_2_json(data_set_url)

    # Client (add connection string)
    client = pymongo.MongoClient("")

    # Use DB
    data_base = client.act1

    insert_to_mongo(data_base, json)
    get_accidents_grouped_by_day_and_time(data_base)

    get_top_neighbourhoods_with_more_accidents(data_base)

    get_license_years_mean(data_base)

    print("Finish script...")