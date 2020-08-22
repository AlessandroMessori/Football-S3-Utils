import json
from src.BucketHelper import BucketHelper

with open('./dags/utils/config.json', 'r') as f:
    config = json.load(f)
    bucket_helper = BucketHelper("football-news")

    languages = config["languages"]

    for lan in languages:
        print(lan)
        bucket_helper.update_bucket_structure(lan['id'])
        bucket_helper.upload_daily_data("/usr/local/airflow/data/" + lan['name'] + ".csv", lan['id'])
