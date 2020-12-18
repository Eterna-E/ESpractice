from elasticsearch import Elasticsearch
import json

def get_query(gpsid):
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
            "statistics": {
                "scripted_metric": {
                    "params":{
                        "m" : gpsid
                    },
                    "init_script": """
                    state.transactions = new HashMap();
                    state.transactions.put("all", new HashMap());
                    state.transactions.put("mall", new HashMap());
                    """,
                    "map_script": """
                    for(s in doc['user_info']){
                        String key = s.substring(0, s.indexOf(","));
                        int value = Integer.parseInt(s.substring(s.indexOf(",")+1));
                        if(!state.transactions.get("all").containsKey(key)){
                            state.transactions.get("all").put(key, value);
                        }
                        else{
                            state.transactions.get("all").put(key, state.transactions.get("all").get(key) + value);
                        }
                    }
                    """,
                    "combine_script": """
                    for(key in params.m){
                        if(state.transactions.get("all").containsKey(key)){
                            state.transactions.get("mall").put(key, state.transactions.get("all").get(key));
                        }
                    }
                    return state.transactions;
                    """,
                    "reduce_script": """
                    Map result = new HashMap();
                    result.put("all", new HashMap());
                    result.put("mall", new HashMap());
                    int msum = 0;
                    int allsum = 0;
                    for(data_buffer in states){
                        for(key in data_buffer.get("all").keySet()){
                            int val = data_buffer.get("all").get(key);
                            allsum += val;
                            if(!result.get("all").containsKey(key)){
                                result.get("all").put(key, val);
                            }
                            else{
                                result.get("all").put(key, result.get("all").get(key) + val);
                            }
                        }
                        for(key in data_buffer.get("mall").keySet()){
                            int val = data_buffer.get("mall").get(key);
                            msum += val;
                            if(!result.get("mall").containsKey(key)){
                                result.get("mall").put(key, val);
                            }
                            else{
                                result.get("mall").put(key, result.get("mall").get(key) + val);
                            }
                        }
                    }
                    Map number = new HashMap();
                    number.put("所有人數", result.get("all").size());
                    number.put("所有次數", allsum);
                    number.put("受眾人數", result.get("mall").size());
                    number.put("受眾次數", msum);
                    return number;
                    """
                }
            }
        }
    }
    return query

if __name__ == "__main__":
    es = Elasticsearch(hosts='10.20.1.91', port=9200)
    with open("gpsid.json", "r") as f:
        gpsid = json.load(f)["gpsid"]
    data = es.search(index="anal_product_brand_2020-02" ,body=get_query(gpsid), size=0)
    #print(json.dumps(data, ensure_ascii=False))
    #with open('agg.txt', 'w', encoding='utf8') as f:
    #    f.write(json.dumps(data, ensure_ascii=False, indent=4))
    #with open("gpsid.json", "r") as f:
    #    data = json.load(f)
    print(json.dumps(data, ensure_ascii=False, indent=4))