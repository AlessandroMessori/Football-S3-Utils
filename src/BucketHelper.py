import boto3
from datetime import date


class BucketHelper:

    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.s3 = boto3.resource('s3')
        self.bucket = self.s3.Bucket(bucket_name)
        self.bucket_tree = self.get_bucket_tree()

    def get_bucket_tree(self):
        bucket_tree = dict()
        self.bucket = boto3.resource('s3').Bucket(self.bucket_name)

        for bucket_object in self.bucket.objects.all():
            full_object_name = bucket_object.key
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
            self.s3.Object(self.bucket_name, language + '/' + year + '/').put(Body='')
            self.s3.Object(self.bucket_name, language + '/' + year + '/' + month + '/').put(Body='')
            self.get_bucket_tree()
        elif month not in self.bucket_tree[language][year]:
            print("adding current month to folder structure...")
            self.s3.Object(self.bucket_name, language + '/' + year + '/' + month + '/').put(Body='')
            self.get_bucket_tree()

    def upload_daily_data(self, file_path, language):
        today = date.today()
        (year, month, day) = str(today).split("-")

        self.update_bucket_structure(language)
        print("uploading current day to S3")
        boto3.client('s3').upload_file(file_path, self.bucket_name,
                                       language + '/' + year + '/' + month + '/' + day + ".csv")
