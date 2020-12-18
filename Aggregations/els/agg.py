from elasticsearch import Elasticsearch
import json

def get_query():
    query = {
        "query": {
            "bool": {
                "filter": [
                    {
                        "term": {
                            "category": "lv1_b"
                        }
                    },
                    {
                        "term": {
                            "time_type": "w"
                        }
                    },
                    {
                        "term": {
                            "start_date": "2020-02-01"
                        }
                    },
                    {
                        "term": {
                            "end_date": "2020-02-01"
                        }
                    }
                ]
            }
        },
        "aggs": {
            "class_avg": {
                "scripted_metric": {
                    "params": {
                        "add_point": 10
                    },
                    "init_script": "state.transactions = new HashSet();",
                    "map_script": """
                    state.transactions.add(doc['user_info']);
                    """,
                    "combine_script": "return new ArrayList(state.transactions);",
                    "reduce_script": """
                    return states
                    """
                }
            }
        }
    }
    return query

if __name__ == "__main__":
    es = Elasticsearch(hosts='10.20.1.91', port=9200)
    data = es.search(index="anal_product_brand_2020-02" ,body=get_query())
    print(json.dumps(data, ensure_ascii=False))