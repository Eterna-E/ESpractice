from elasticsearch import Elasticsearch
import json
import datetime
import calendar


class ProductAnalyse(object):
    def __init__(self, payload, debug=False):
        self.payload = payload
        self.debug = debug

    def get_query(self):
        query = {"query": BehaviorParser(self.payload).parser()}
        if self.debug:
            with open("query.json", "w", encoding="utf-8") as f:
                f.write(json.dumps(query, indent=4, ensure_ascii=False))
        return query

    def analyse(self):
        query = self.get_query()
        query["aggs"] = {
            "statistics": {
                "scripted_metric": {
                    "params": {
                        "gpsid_list": self.payload.get("gpsid_list"),
                        "lv1_list": self.payload.get("lv1_list")
                    },
                    "init_script": """
                    state.transactions = new HashMap();
                    state.transactions.put("感興趣", new HashMap());
                    state.transactions.put("考慮購買", new HashMap());
                    state.transactions.put("完成購買", new HashMap());
                    for(motivate in state.transactions.keySet()){
                        for(key in params.lv1_list.keySet()){
                            state.transactions.get(motivate).put(key, new HashMap());
                            for(name in params.lv1_list.get(key)){
                                Map lv = new HashMap();
                                lv.put("all", new HashMap());
                                state.transactions.get(motivate).get(key).put(name, lv);
                            }
                        }
                    }
                    """,
                    "map_script": """
                    Map temp;
                    if(doc['motivation'].value == 0){
                        temp = state.transactions.get("感興趣");
                    } else if(doc['motivation'].value == 1){
                        temp = state.transactions.get("考慮購買");
                    } else{
                        temp = state.transactions.get("完成購買");
                    }
                    String name = doc['name'].value;
                    if(doc['category'].value.equals("lv1")){
                        temp = temp.get("all").get(name);
                    } else{
                        String key = name.substring(0, name.indexOf("|"));
                        temp = temp.get(key).get(name);
                    }
                    temp = temp.get("all");
                    for(s in doc['user_info']){
                        String sid = s.substring(0, s.indexOf(","));
                        int value = Integer.parseInt(s.substring(s.indexOf(",")+1));
                        if(!temp.containsKey(sid)){
                            temp.put(sid, value);
                        }
                        else{
                            temp.put(sid, temp.get(sid) + value);
                        }
                    }
                    """,
                    "combine_script": """
                    for(motivate in state.transactions.keySet()){
                        Map motivation = state.transactions.get(motivate);
                        for(key in motivation.keySet()){
                            Map temp = motivation.get(key);
                            for(name in temp.keySet()){
                                Map lv = temp.get(name);
                                lv.put("mall", new HashMap());
                                Map all = lv.get("all");
                                Map mall = lv.get("mall");
                                for(gpsid in params.gpsid_list){
                                    if(all.containsKey(gpsid)){
                                        mall.put(gpsid, all.get(gpsid));
                                    }
                                }
                            }
                        }
                    }
                    return state.transactions;
                    """,
                    "reduce_script": """
                    Map result = states.remove(0);
                    while (states.size() > 0){
                        Map data_buffer = states.remove(0);
                        for(motivate in data_buffer.keySet()){
                            Map motivation = data_buffer.get(motivate);
                            Map result_motivation = result.get(motivate);
                            for(key in motivation.keySet()){
                                Map data_buffer_temp = motivation.get(key);
                                Map result_temp = result_motivation.get(key);
                                for(name in data_buffer_temp.keySet()){
                                    Map data_buffer_all = data_buffer_temp.get(name).get("all");
                                    Map result_all = result_temp.get(name).get("all");
                                    for(sid in data_buffer_all.keySet()){
                                        int value = data_buffer_all.get(sid);
                                        if(!result_all.containsKey(sid)){
                                            result_all.put(sid, value);
                                        }
                                        else{
                                            result_all.put(sid, result_all.get(sid) + value);
                                        }
                                    }
                                    Map data_buffer_mall = data_buffer_temp.get(name).get("mall");
                                    Map result_mall = result_temp.get(name).get("mall");
                                    for(sid in data_buffer_mall.keySet()){
                                        int value = data_buffer_mall.get(sid);
                                        if(!result_mall.containsKey(sid)){
                                            result_mall.put(sid, value);
                                        }
                                        else{
                                            result_mall.put(sid, result_mall.get(sid) + value);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    for(motivate in result.keySet()){
                        Map motivation = result.get(motivate);
                        for(key in motivation.keySet()){
                            Map temp = motivation.get(key);
                            for(name in temp.keySet()){
                                Map lv = temp.get(name);
                                int all = 0;
                                int mall = 0;
                                for(sid in lv.get("all").keySet()){
                                    all += lv.get("all").get(sid);
                                }
                                for(sid in lv.get("mall").keySet()){
                                    mall += lv.get("mall").get(sid);
                                }
                                Map number = new HashMap();
                                number.put("all_uv", lv.get("all").size());
                                number.put("all_vc", all);
                                number.put("uv", lv.get("mall").size());
                                number.put("vc", mall);
                                temp.put(name, number);
                            }
                        }
                    }
                    return result;
                    """
                }
            }
        }
        es = Elasticsearch(hosts='10.20.1.91', port=9200)
        if self.debug:
            with open("aggs_query.json", "w", encoding="utf-8") as f:
                f.write(json.dumps(query, indent=4, ensure_ascii=False))
        aggs = es.search(index="anal_product_cat_2020-02", body=query, size=0)
        if self.debug:
            with open("aggs.json", "w", encoding="utf-8") as f:
                f.write(json.dumps(aggs, indent=4, ensure_ascii=False))
        return aggs

    def get_result(self):
        result = self.analyse()['aggregations']['statistics']['value']
        for motivation in result.values():
            for key, temp in motivation.items():
                anatomy = list()
                for name, lv in temp.items():
                    anatomy.append(
                        {
                            "name": name,
                            "index": lv["uv"] / self.payload["ppl_nb"],
                            "average": lv["all_uv"] / self.payload["all_ppl_nb"]
                        }
                    )
                anatomy.sort(key=lambda k: k["index"], reverse=True)
                for index in range(len(anatomy)):
                    anatomy[index]["rank"] = index + 1
                motivation[key] = {"anatomy": anatomy}
        if self.debug:
            with open("result.json", "w", encoding="utf-8") as f:
                f.write(json.dumps(result, indent=4, ensure_ascii=False))
        return result


class BehaviorParser(object):
    def __init__(self, payload):
        self.payload = payload

    def parser(self):
        lv_query = self.lv_parser()
        slot_query = self.slot_parser()
        time_query = TimeParser(self.payload.get(
            "period_from"), self.payload.get("period_to")).parser()
        parser_query = [lv_query, slot_query, time_query]
        query = {"bool": {"filter": parser_query}}
        return query

    def lv_parser(self):
        lv = list()
        lv1_list = self.payload.get("lv1_list")
        for key, lv_list in lv1_list.items():
            for category in lv_list:
                lv.append(category)
        return {"terms": {"name": lv}}

    def slot_parser(self):
        start, end = self.payload.get("slot_from"), self.payload.get("slot_to")
        term = "term"
        if start == end:
            hr = start
        elif start == 0 and end == 23:
            hr = -1
        else:
            hr = list(range(start, end+1))
            term = "terms"
        return {term: {"hr": hr}}


class TimeParser(object):
    def __init__(self, start_day, end_day):
        self.ranges = list()
        self.start_day = datetime.date.fromisoformat(start_day)
        self.end_day = datetime.date.fromisoformat(end_day)

    def parser(self):
        self.range_parser()
        self.ranges = self.ranges[0] if len(self.ranges) == 1 else self.ranges
        parsed_time = self.ranges if isinstance(self.ranges, dict) else {
            "bool": {"should": self.ranges}}
        return parsed_time

    def range_parser(self):
        while(self.start_day <= self.end_day):
            if self.month_parser():
                continue
            if self.week_parser():
                continue
            self.day_parser()

    def month_parser(self):
        if self.start_day.day == 1:
            last_day_of_month = self.start_day.replace(
                day=calendar.monthlen(
                    self.start_day.year, self.start_day.month))
            if self.end_day >= last_day_of_month:
                self.splice_packer(last_day_of_month, "m")
                self.start_day = last_day_of_month + datetime.timedelta(days=1)
                return True

    def week_parser(self):
        if (self.end_day - self.start_day).days >= 6 and \
                self.start_day.weekday() == 6:
            last_day_of_month = self.start_day.replace(
                day=calendar.monthlen(*self.start_day.timetuple()[:2]))
            if (last_day_of_month - self.start_day).days >= 6:
                self.splice_packer(
                    self.start_day + datetime.timedelta(days=6), "w")
                self.start_day += datetime.timedelta(days=7)
                return True
        if self.start_day.weekday() == 6:
            last_day_of_month = self.start_day.replace(
                day=calendar.monthlen(*self.start_day.timetuple()[:2]))
            if self.end_day >= last_day_of_month and \
                    (last_day_of_month - self.start_day).days < 6:
                self.splice_packer(last_day_of_month, "w")
                self.start_day = last_day_of_month + datetime.timedelta(days=1)
                return True
        if self.start_day.day == 1 and self.start_day.weekday() != 6:
            saturday = self.start_day + \
                datetime.timedelta(days=5-self.start_day.weekday())
            if self.end_day >= saturday:
                self.splice_packer(saturday, "w")
                self.start_day = saturday + datetime.timedelta(days=1)
                return True

    def day_parser(self):
        if self.start_day <= self.end_day:
            self.splice_packer(self.start_day, "d")
            self.start_day += datetime.timedelta(days=1)

    def splice_packer(self, end_day, time_type):
        start_date = {"term": {"start_date": self.start_day.isoformat()}}
        end_date = {"term": {"end_date": end_day.isoformat()}}
        time_type = {"term": {"time_type": time_type}}
        self.ranges.append(
            {"bool": {"filter": [start_date, end_date, time_type]}})


if __name__ == "__main__":
    with open("gpsid_list.json", "r", encoding="utf-8") as f:
        gpsid = json.load(f)["gpsid"][:10000]
    with open("lv.json", "r", encoding="utf-8") as f:
        lv = json.load(f)
    payload = {
        "gpsid_list": gpsid,
        "ppl_nb": len(gpsid),
        "ppl_wnb": 4 * len(gpsid),
        "all_ppl_nb": 10 * len(gpsid),
        "period_from": "2020-02-01",
        "period_to": "2020-02-01",
        "slot_from": 0,
        "slot_to": 23,
        "lv1_list": lv
    }
    result = ProductAnalyse(payload, debug=True).get_result()
