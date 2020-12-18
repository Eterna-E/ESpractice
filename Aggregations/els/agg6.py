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
                    "init_script": "state.transactions = new HashMap();",
                    "map_script": """
                    String name = doc['name'].value;
                    String brand = name.substring(0, name.indexOf("|"));
                    if(!state.transactions.containsKey(brand)){
                        state.transactions.put(brand, new HashMap());
                        state.transactions.get(brand).put("all", new HashMap());
                        state.transactions.get(brand).put("mall", new HashMap());
                    }
                    for(s in doc['user_info']){
                        String key = s.substring(0, s.indexOf(","));
                        int value = Integer.parseInt(s.substring(s.indexOf(",")+1));
                        if(!state.transactions.get(brand).get("all").containsKey(key)){
                            state.transactions.get(brand).get("all").put(key, value);
                        }
                        else{
                            state.transactions.get(brand).get("all").put(key, state.transactions.get(brand).get("all").get(key) + value);
                        }
                    }
                    """,
                    "combine_script": """
                    for(brand in state.transactions.keySet()){
                        for(key in params.m){
                            if(state.transactions.get(brand).get("all").containsKey(key)){
                                state.transactions.get(brand).get("mall").put(key, state.transactions.get(brand).get("all").get(key));
                            }
                        }
                    }
                    return state.transactions;
                    """,
                    "reduce_script": """
                    Map result = states.remove(0);
                    while (states.size() > 0){
                        Map data_buffer = states.remove(0);
                        for(brand in data_buffer.keySet()){
                            if(!result.containsKey(brand)){
                                result.put(brand, data_buffer.get(brand));
                            }
                            else{
                                for(key in data_buffer.get(brand).get("all").keySet()){
                                    int value = data_buffer.get(brand).get("all").get(key);
                                    if(!result.get(brand).get("all").containsKey(key)){
                                        result.get(brand).get("all").put(key, value);
                                    }
                                    else{
                                        result.get(brand).get("all").put(key, result.get(brand).get("all").get(key) + value);
                                    }
                                }
                                for(key in data_buffer.get(brand).get("mall").keySet()){
                                    int value = data_buffer.get(brand).get("mall").get(key);
                                    if(!result.get(brand).get("mall").containsKey(key)){
                                        result.get(brand).get("mall").put(key, value);
                                    }
                                    else{
                                        result.get(brand).get("mall").put(key, result.get(brand).get("mall").get(key) + value);
                                    }
                                }
                            }
                        }
                    }
                    for(brand in result.keySet()){
                        int all = 0;
                        int mall = 0;
                        for(key in result.get(brand).get("all").keySet()){
                            all += result.get(brand).get("all").get(key);
                        }
                        for(key in result.get(brand).get("mall").keySet()){
                            mall += result.get(brand).get("mall").get(key);
                        }
                        Map number = new HashMap();
                        number.put("所有人數", result.get(brand).get("all").size());
                        number.put("所有次數", all);
                        number.put("受眾人數", result.get(brand).get("mall").size());
                        number.put("受眾次數", mall);
                        result.put(brand, number);
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
    with open("gpsid.json", "r") as f:
        gpsid = json.load(f)["gpsid"]
    data = es.search(index="anal_product_brand_2020-02" ,body=get_query(gpsid), size=0)
    #print(json.dumps(data, ensure_ascii=False))
    with open('agg.txt', 'w', encoding='utf8') as f:
        f.write(json.dumps(data, ensure_ascii=False, indent=4))
    #with open("gpsid.json", "r") as f:
    #    data = json.load(f)
    print(json.dumps(data, ensure_ascii=False, indent=4))