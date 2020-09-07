import json
from src.BucketHelper import BucketHelper
import sys

with open('/usr/local/airflow/config.json', 'r') as f:
    config = json.load(f)
    bucket_helper = BucketHelper(sys.argv[1])

    languages = config["languages"]

    for lan in languages:
        print(lan)
        bucket_helper.update_bucket_structure(lan['id'])
        bucket_helper.upload_daily_data("/usr/local/airflow/" + sys.argv[2] + "/" + lan['name'] + ".csv", lan['id'])
