from src.BucketHelper import BucketHelper

bucket_helper = BucketHelper("football-news")

bucket_helper.update_bucket_structure("en")
bucket_helper.list_bucket_structure()

