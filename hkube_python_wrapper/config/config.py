import os


def getIntEnv(name, defaultValue):
    strValue = os.environ.get(name, defaultValue)
    return int(strValue)


def getBoolEnv(name, defaultValue):
    strValue = os.environ.get(name, defaultValue)
    return strValue.lower() == 'true'


socket = {
    "port": os.environ.get('WORKER_SOCKET_PORT', "3000"),
    "host": os.environ.get('WORKER_SOCKET_HOST', "127.0.0.1"),
    "protocol": os.environ.get('WORKER_SOCKET_PROTOCOL', "ws"),
    "url": os.environ.get('WORKER_SOCKET_URL', None),
    "encoding": os.environ.get('WORKER_ALGORITHM_ENCODING', 'bson')
}
discovery = {
    "host": os.environ.get('POD_IP', '127.0.0.1'),
    "port": os.environ.get('DISCOVERY_PORT', 9020),
    "encoding": os.environ.get('DISCOVERY_ENCODING', 'msgpack'),
    "enable": getBoolEnv('DISCOVERY_ENABLE', 'True'),
    "timeout": getIntEnv('DISCOVERY_TIMEOUT', 10000),
    "networkTimeout": getIntEnv('DISCOVERY_NETWORK_TIMEOUT', 1000),
    "maxCacheSize": getIntEnv('DISCOVERY_MAX_CACHE_SIZE', 400),
    "num_threads": getIntEnv('DISCOVERY_SERVER_NUM_THREADS', 5),
    "num_ping_threads": getIntEnv('DISCOVERY_SERVER_NUM_PING_THREADS', 5),
    "servingReportInterval": getIntEnv('DISCOVERY_SERVING_REPORT_INTERVAL', 5000),
    "streaming": {
        "messagesMemoryBuff": getIntEnv('STREAMING_MAX_BUFFER_MB', 1500),
        "port": os.environ.get('STREAMING_DISCOVERY_PORT', 9022),
        "statisticsInterval": os.environ.get('STREAMING_STATISTICS_INTERVAL', 2),
        "stateful": getBoolEnv('STREAMING_STATEFUL', 'True')
    }

}
algorithm = {
    "path": os.environ.get('ALGORITHM_PATH', "algorithm_unique_folder"),
    "entryPoint": os.environ.get('ALGORITHM_ENTRY_POINT', "main.py"),
}
storage = {
    "clusterName": os.environ.get('CLUSTER_NAME', 'local'),
    "type": os.environ.get('DEFAULT_STORAGE', 'fs'),
    "mode": os.environ.get('STORAGE_PROTOCOL', 'v3'),
    "encoding": os.environ.get('STORAGE_ENCODING', 'msgpack'),
    "maxCacheSize": getIntEnv('STORAGE_MAX_CACHE_SIZE', 400),
    "fs": {
        "baseDirectory": os.environ.get('BASE_FS_ADAPTER_DIRECTORY', '/var/tmp/fs/storage')
    },
    "s3": {
        "accessKeyId": os.environ.get('AWS_ACCESS_KEY_ID', 'AKIAIOSFODNN7EXAMPLE'),
        "secretAccessKey": os.environ.get('AWS_SECRET_ACCESS_KEY', 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'),
        "endpoint": os.environ.get('S3_ENDPOINT_URL', 'http://127.0.0.1:9000'),
        "region": os.environ.get('S3_REGION', ''),
        "bucketName": os.environ.get('S3_BUCKET_NAME', None),
    }
}
tracer = {
    "config": {
        'sampler': {
            'type': 'const',
            'param': 1,
        },
        'local_agent': {
            'reporting_host': os.environ.get('JAEGER_AGENT_SERVICE_HOST', 'localhost'),
            'reporting_port': os.environ.get('JAEGER_AGENT_SERVICE_PORT_JAEGER_COMPACT', "6831"),
        },
        'logging': True,
    },
    "service_name": os.environ.get('ALGORITHM_TYPE', 'algorithm')
}
logging = {
    "level": os.environ.get('HKUBE_LOG_LEVEL', "INFO")
}
