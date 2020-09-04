from requests import request
from requests.exceptions import RequestException
import xml.etree.ElementTree as ET
from datetime import date


class BucketHelper:

    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.objects = list()
        self.get_objects()
        self.bucket_tree = self.get_bucket_tree()

    def get_objects(self):
        response = request("GET", "https://" + self.bucket_name + ".s3.amazonaws.com")

        root = ET.fromstring(response.text)

        for item in root:
            for sub_item in item:
                if sub_item.tag[-3:] == "Key":
                    self.objects.append(sub_item.text)

    def get_bucket_tree(self):
        bucket_tree = dict()

        for full_object_name in self.objects:
            # print(full_object_name)
            tree_level = full_object_name.count('/')

            if tree_level == 1:
                language = full_object_name.split("/")[0]
                bucket_tree[language] = dict()
            elif tree_level == 2:
                language = full_object_name.split("/")[0]
                year = full_object_name.split("/")[1]
                bucket_tree[language][year] = dict()
            elif tree_level == 3:
                language = full_object_name.split("/")[0]
                year = full_object_name.split("/")[1]
                month = full_object_name.split("/")[2]
                bucket_tree[language][year][month] = list()
            elif tree_level == 4:
                language = full_object_name.split("/")[0]
                year = full_object_name.split("/")[1]
                month = full_object_name.split("/")[2]
                day = full_object_name.split("/")[3]
                bucket_tree[language][year][month].append(day)

        return bucket_tree

    def print_bucket_tree(self, tree, level=0):
        padding = '  '.join(['' for i in range(0, level + 1)])

        if level < 3:
            for key in tree.keys():
                print(padding + '- ' + key)
                self.print_bucket_tree(tree[key], level + 1)
        elif level == 4:
            for item in tree:
                print(padding + '- ' + item)

    def list_bucket_structure(self):
        print(self.bucket_name + ' Bucket Structure:')

        bucket_tree = self.get_bucket_tree()

        self.print_bucket_tree(bucket_tree)

    def update_bucket_structure(self, language):
        today = date.today()
        (year, month, day) = str(today).split("-")

        if year not in self.bucket_tree[language]:
            print("adding current year and month to folder structure...")
            try:
                request("PUT", "https://" + self.bucket_name + ".s3.amazonaws.com" + language + '/' + year + '/')
                request("PUT",
                        "https://" + self.bucket_name + ".s3.amazonaws.com" + language + '/' + year + '/' + month + '/')
                self.get_bucket_tree()
            except RequestException as e:
                print(e)
        elif month not in self.bucket_tree[language][year]:
            print("adding current month to folder structure...")
            try:
                request("PUT",
                        "https://" + self.bucket_name + ".s3.amazonaws.com" + '/' + language + '/' + year + '/' + month + '/',
                        params={'body': ''})
                self.get_bucket_tree()
            except RequestException as e:
                print(e)

    def upload_daily_data(self, file_path, language):
        today = date.today()
        (year, month, day) = str(today).split("-")

        self.update_bucket_structure(language)

        with open(file_path) as fh:
            file_content = fh.read()
            print("uploading current day to S3")
            request("PUT",
                    "https://" + self.bucket_name + ".s3.amazonaws.com" + '/' + language + '/' + year + '/' + month + '/' + day + '.csv',
                    data=file_content,
                    params={'file': file_path})

