from elasticsearch import Elasticsearch
import json

def change_mappings(es):
    body = get_teacher_mappings()
    es.indices.put_mapping(index='school_members_2', body=body)

def get_teacher_mappings():
    mappings = {
        "properties": {
            "tid": {
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
            },
            "salary": {
                "type": "integer"
            }
        }
    }
    return mappings

if __name__ == "__main__":
    es = Elasticsearch(hosts='192.168.1.59', port=9200)
    change_mappings(es)