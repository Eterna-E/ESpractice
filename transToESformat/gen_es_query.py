# 轉換文字格式(payload)為ES搜尋格式
import es_query as es_q
import dateProcess as dp
import json

class ESquery(object):

    def content_parser(self, payload):
        datasource = {
            "bool":{
                "filter":{
                    "term":{
                        "source":payload.get("datasource")
                    }
                }
            }
        }
        label_query = self.label_parser(payload.get("label_setting"))
        time_parser = self.time_parser(payload.get("time_setting"))
        query = {"bool": {"filter": [time_parser,label_query,datasource]}}
        return query
        
    
    def time_parser(self, time_payload):
        time_query = {
            "bool":{
                "should":[]
            }
        }
        start_dt, end_dt = time_payload.get("range").split('-')
        # print(start_dt, end_dt)
        weeks = dp.date_to_week(start_dt,end_dt)
        # print(weeks)
        for i,week in enumerate(weeks):
            print(i,week)
            time_term = []
            if i == 0:
                time_term.append(es_q.term_query("start_dt",week[0]))
                time_term.append(es_q.term_query("end_dt",week[-1]))
                time_term.append(es_q.term_query("time_type","w"))
                time_query["bool"]["should"].append(es_q.filter_query(time_term))
            elif len(week) == 7:
                time_term.append(es_q.term_query("start_dt",week[0]))
                time_term.append(es_q.term_query("end_dt",week[-1]))
                time_term.append(es_q.term_query("time_type","w"))
                time_query["bool"]["should"].append(es_q.filter_query(time_term))
            if i == len(weeks)-1 and len(week) != 7 and len(weeks) != 1:
                for day in week:
                    time_term = []
                    time_term.append(es_q.term_query("start_dt",day))
                    time_term.append(es_q.term_query("end_dt",day))
                    time_term.append(es_q.term_query("time_type","d"))
                    time_query["bool"]["should"].append(es_q.filter_query(time_term))
        return time_query
    
    def label_parser(self, labels):
        label_query = {
            "bool":{
                "should":[]
            }
        }
        lv_query = None
        for label in labels:
            main = label.get("main_label")
            label1, label2 = main.get("label1"), main.get("label2")
            if label1:
                lv_query = None
                if label2:
                    label1, label2 = es_q.term_query("label1", label1), es_q.term_query("label2", label2)
                    lv_query = es_q.filter_query([label1, label2])
                else:
                    lv_query = es_q.term_query("label1", label1)
            label_query["bool"]["should"].append(lv_query)

        return label_query

if __name__ == "__main__":
    payload = {
        "datasource": "Behavior Tree",
        "label_setting": [
            {
                "main_label": {
                    "label1": "社群",
                    "label2": ["論壇"]
                },
                "brands": []
            },
            {
                "main_label": {
                    "label1": "服裝與飾品"
                },
                "brands": []
            }
        ],
        "time_setting": {
            "range": "2020/01/27-2020/02/15",
            "times": "1次"
        }
    }

    es_query = ESquery().content_parser(payload)
    
    print(es_query)

    jsonStr = json.dumps(es_query, indent=1)
    # print(jsonStr)
    with open('output.json','w') as f:
        f.write(jsonStr)