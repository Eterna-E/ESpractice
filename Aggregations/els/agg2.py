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
            "user_info_reduce": {
                "scripted_metric": {
                    "init_script": "state.transactions = new ArrayList();",
                    "map_script": """
                    for(user_info in doc['user_info']){
                        state.transactions.add(user_info);
                    }
                    """,
                    "combine_script": "return new ArrayList(state.transactions);",
                    "reduce_script": """
                    Map result = new HashMap();
                    for(data_buffer in states){
                        for(s in data_buffer){
                            def key = s.substring(0, s.indexOf(","));
                            def value = Integer.parseInt(s.substring(s.indexOf(",")+1));
                            if(!result.containsKey(key)){
                                result.put(key, value);
                            }
                            else{
                                result.put(key, result.get(key) + value);
                            }
                        }
                    }
                    return result;
                    """
                }
            }
        }
    }
    return query

if __name__ == "__main__":
    es = Elasticsearch(hosts='10.20.1.91', port=9200)
    data = es.search(index="anal_product_brand_2020-02" ,body=get_query(), size=0)
    #print(json.dumps(data, ensure_ascii=False))
    with open('agg.txt', 'w', encoding='utf8') as f:
        f.write(json.dumps(data, ensure_ascii=False, indent=4))
    #print(len(data["aggregations"]["user_info_reduce"]["value"]))