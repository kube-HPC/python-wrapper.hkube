import os
import boto3


class S3Adapter:
    def __init__(self, config):
        accessKeyId = config["accessKeyId"]
        secretAccessKey = config["secretAccessKey"]
        endpoint = config["endpoint"]

        self.client = boto3.client(
            's3',
            aws_access_key_id=accessKeyId,
            aws_secret_access_key=secretAccessKey,
            endpoint_url=endpoint
        )

    def init(self, options):
        return self.createBucket(options)

    def createBucket(self, options):
        bucket = options["bucket"]
        if(self.isBucketExist(bucket) is False):
            return self.client.create_bucket(Bucket=bucket)
        return None

    def isBucketExist(self, bucket):
        try:
            self.client.head_bucket(Bucket=bucket)
            return True
        except Exception as e:
            if (hasattr(e, 'response') and e.response['Error']['Code'] == '404'):
                return False
            raise e

    def put(self, options):
        path = options["path"]
        body = options["data"]
        parsedPath = self._parsePath(path)
        bucket = parsedPath["bucket"]
        key = parsedPath["key"]
        self.client.put_object(Bucket=bucket, Key=key, Body=body)
        return {'path': bucket + os.path.sep + key}

    def get(self, options):
        path = options["path"]
        parsedPath = self._parsePath(path)
        bucket = parsedPath["bucket"]
        key = parsedPath["key"]
        response = self.client.get_object(Bucket=bucket, Key=key)
        data = response['Body'].read()
        return data

    def list(self, options):
        path = options["path"]
        parsedPath = self._parsePath(path)
        bucket = parsedPath["bucket"]
        response = self.client.list_objects_v2(Bucket=bucket)
        data = list(map(lambda x: {"path": bucket + os.path.sep + x['Key']}, response['Contents']))
        return data

    def _parsePath(self, fullPath):
        seperatedPath = fullPath.split(os.path.sep)
        bucket = seperatedPath[0]
        key = seperatedPath[1:]
        return {"bucket": bucket, "key": os.path.sep.join(key)}
