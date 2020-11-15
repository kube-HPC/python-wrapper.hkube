import os


def getIntEnv(name, defaultValue):
    strValue = os.environ.get(name, defaultValue)
    return int(strValue)


socket = {
    "port": os.environ.get('WORKER_SOCKET_PORT', "3001"),
    "host": os.environ.get('WORKER_SOCKET_HOST', "127.0.0.1"),
    "protocol": os.environ.get('WORKER_SOCKET_PROTOCOL', "ws"),
    "url": os.environ.get('WORKER_SOCKET_URL', None),
    "encoding": os.environ.get('WORKER_ALGORITHM_ENCODING', 'json'),
}
discovery = {
    "host": os.environ.get('POD_NAME', '127.0.0.1'),
    "port": os.environ.get('DISCOVERY_PORT', "9020"),
    "encoding": os.environ.get('DISCOVERY_ENCODING', 'msgpack'),
    "timeout": getIntEnv('DISCOVERY_TIMEOUT', 5000),
    "networkTimeout": getIntEnv('DISCOVERY_NETWORK_TIMEOUT', 1000),
    "maxCacheSize": getIntEnv('DISCOVERY_MAX_CACHE_SIZE', 400)
}
algorithm = {
    "path": os.environ.get('ALGORITHM_PATH', "algorithm_unique_folder"),
    "entryPoint": os.environ.get('ALGORITHM_ENTRY_POINT', "main.py")
}
storage = {
    "clusterName": os.environ.get('CLUSTER_NAME', 'local'),
    "type": os.environ.get('STORAGE_TYPE', 'fs'),
    "mode": os.environ.get('STORAGE_PROTOCOL', 'v3'),
    "encoding": os.environ.get('STORAGE_ENCODING', 'msgpack'),
    "maxCacheSize": getIntEnv('STORAGE_MAX_CACHE_SIZE', 400),
    "fs": {
        "baseDirectory": os.environ.get('BASE_FS_ADAPTER_DIRECTORY', 'var/tmp/fs/storage')
    },
    "s3": {
        "accessKeyId": os.environ.get('AWS_ACCESS_KEY_ID', 'AKIAIOSFODNN7EXAMPLE'),
        "secretAccessKey": os.environ.get('AWS_SECRET_ACCESS_KEY', 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'),
        "endpoint": os.environ.get('AWS_ENDPOINT', 'http://127.0.0.1:9000')
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
            'reporting_port': os.environ.get('JAEGER_AGENT_SERVICE_PORT_AGENT_BINARY', os.environ.get('JAEGER_AGENT_SERVICE_PORT', "6831")),
        },
        'logging': True,
    },
    "service_name": os.environ.get('ALGORITHM_NAME', 'algorithm')
}
