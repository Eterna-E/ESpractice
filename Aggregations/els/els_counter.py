from elasticsearch import Elasticsearch
import json

def create_index(es):
    body = dict()
    body['settings'] = get_setting()
    body['mappings'] = get_mappings()
    print(json.dumps(body)) #可以用json.dumps輸出來看格式有沒又包錯
    es.indices.create(index='school_members_2', body=body)


def get_setting():
    settings = {
        "number_of_shards": 3,
        "number_of_replicas": 2
    }

    return settings


def get_mappings():
    mappings = {
        "properties": {
            "sid": {
                "type": "integer"
            },
            "name": {
                "type": "text"
            },
            "age": {
                "type": "integer"
            },
            "class": {
                "type": "keyword"
            }
        }
    }

    return mappings


if __name__ == "__main__":
    es = Elasticsearch(hosts='192.168.1.59', port=9200)
    create_index(es)