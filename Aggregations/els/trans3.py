import json
import datetime
import calendar

class BehaviorParser(object):
    def parser(self, payload):
        source = {"term": {"source": payload.get("datasource")}}
        source_query = {"bool": {"filter": source}}
        label_query = self.label_parser(payload.get("label_setting"))
        time_query = TimeParser().parser(payload.get("time_setting"))
        parser_query = [source_query, label_query, time_query]
        query = {"bool": {"filter": parser_query}}
        return query

    def label_parser(self, label_settings):
        labels = list()
        for label_setting in label_settings:
            term_terms = list()
            for key, label in label_setting.get("main_label").items():
                condition = "terms" if isinstance(label, list) else "term"
                term_terms.append({condition: {key.replace("label", "lv"): label}})
            term_terms = term_terms[0] if len(term_terms) == 1 else term_terms
            labels.append({"bool": {"filter": term_terms}})
        parsed_label = {"bool": {"should": labels}}
        #print(json.dumps(parsed_label, indent=4, ensure_ascii=False))
        return parsed_label

class TimeParser(object):
    def parser(self, time_settings):
        times = int(time_settings.get("times").replace("次", ""))
        times = {"term": {"times": times}}
        date = list(map(lambda x: x.replace("/", "-"), time_settings.get("range").split("-")))
        self.start_day, self.end_day = date[0], date[1]
        self.start_day = datetime.date.fromisoformat(self.start_day)
        self.end_day = datetime.date.fromisoformat(self.end_day)
        self.range_parser()
        parsed_time = {"bool": {"filter": [self.labels, times]}}
        #print(json.dumps(parsed_time, indent=4, ensure_ascii=False))
        return parsed_time

    def range_parser(self):
        self.labels = list()
        while(self.start_day <= self.end_day):
            self.month_parser()
            self.week_parser()
            self.day_parser()
        parsed_range = {"bool": {"filter": self.labels}}
        return parsed_range

    def month_parser(self):
        pass

    def week_parser(self):
        pass

    def day_parser(self):
        pass

if __name__ == "__main__":
    payload = {
        "datasource": "Behavior Tree",
        "label_setting": [
            {
                "main_label": {
                    "label1": "社群",
                    "label2": [
                        "論壇"
                    ]
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
            "range": "2020/02/27-2020/02/02",
            "times": "1次"
        }
    }
    query = BehaviorParser().parser(payload)
    print(json.dumps(query, indent=4, ensure_ascii=False))