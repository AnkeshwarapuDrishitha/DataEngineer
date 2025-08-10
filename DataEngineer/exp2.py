import pandas as pd
from pymongo import MongoClient
import schedule
MONGO_URL="mongodb://localhost:27017"
DATABASE_NAME="Employees"
COLLECTION_NAME="user_det"

def extract(path="user.csv"):
    print("Extracting...")
    return pd.read_csv(path)
def transform(df):
    print("clean and transforming...")
    df['emailid']=df['emailid'].str.lower()
    email_pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    df['invalid_email'] = ~df['emailid'].str.match(email_pattern)
    return df
def load_mongodb(df):
    client=MongoClient(MONGO_URL)
    db=client[DATABASE_NAME]
    collection=db[COLLECTION_NAME]
    collection.drop()
    collection.insert_many(df.to_dict(orient='records'))
    print("Load completed")

def run_etl():
    df=extract()
    data=transform(df)
    load_mongodb(df)
if __name__ =="__main__":
    run_etl()
