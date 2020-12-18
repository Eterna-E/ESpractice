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
        }
    }
    return query


if __name__ == "__main__":
    """
    es = Elasticsearch(hosts='10.20.1.91', port=9200)
    query = get_query()
    data = es.search(body=query, size=234850)
    data = json.dumps(data)
    with open('pretty.json', 'w', encoding='utf-8') as f:
        f.write(json.dumps(data, indent=4, ensure_ascii=False))
    """
    """
    with open('pretty.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
    user_info = []
    for item in data["hits"]["hits"]:
        user_info += item["_source"]["user_info"]
    """
    #user_info = list(map(lambda x: (x.split(',')[0], int(x.split(',')[1])), user_info))
    #with open('table.txt', 'w') as f:
    #    f.writelines(list(map(lambda x: x + "\n", user_info)))
    #print(user_info)
    with open('table.txt', 'r') as f:
        data = f.readlines()
    reduced = {}
    for i in data:
        i = i.split(",")
        key, value = i[0], int(i[1])
        if key in reduced:
            reduced[key] += value
        else:
            reduced[key] = value
    with open("reducedpy.txt", "w") as f:
        f.writelines(list(map(lambda x: x + "," + str(reduced[x]) + "\n", reduced)))