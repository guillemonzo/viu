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


def insert_to_mongo(records):

    # Client (add connection string)
    client = pymongo.MongoClient("")

    # Get DB
    db = client.act1

    # Get collection
    collection = db.collection

    # Convert from string to json object
    records = js.loads(records, encoding="utf-8")

    # Delete previous
    collection.delete_many({})

    # Insert documents
    collection.insert_many(records)


if __name__ == "__main__":
    data_set_url = 'https://opendata-ajuntament.barcelona.cat/data/dataset/317e3743-fb79-4d2f-a128-5f12d2c9a55a/resource/6e2daeb5-e359-43ad-b0b5-7fdf438c8d6f/download/2018_accidents_vehicles_gu_bcn_.csv'
    json = csv_2_json(data_set_url)
    insert_to_mongo(json)
    print("Finish script...")