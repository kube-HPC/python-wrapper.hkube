import os
import boto3
from hkube_python_wrapper.util.encoding import Encoding

class S3Adapter:
    def __init__(self, config, _):
        accessKeyId = config["accessKeyId"]
        secretAccessKey = config["secretAccessKey"]
        endpoint = config["endpoint"]

        self.client = boto3.client(
            's3',
            aws_access_key_id=accessKeyId,
            aws_secret_access_key=secretAccessKey,
            endpoint_url=endpoint,
            verify=config["verify_ssl"]
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
        header = options.get("header")
        metadata = {}
        if(header):
            header = Encoding.headerToString(header)
            metadata = {"header": header}
        parsedPath = self._parsePath(path)
        bucket = parsedPath["bucket"]
        key = parsedPath["key"]
        self.client.put_object(Bucket=bucket, Key=key, Body=body, Metadata=metadata)
        return {'path': bucket + os.path.sep + key}

    def get(self, options):
        path = options["path"]
        parsedPath = self._parsePath(path)
        bucket = parsedPath["bucket"]
        key = parsedPath["key"]
        response = self.client.get_object(Bucket=bucket, Key=key)
        payload = response['Body'].read()
        metadata = response.get('Metadata')
        header = None
        if(metadata):
            header = metadata.get('header')
            header = Encoding.headerFromString(header)

        return (header, payload)

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
