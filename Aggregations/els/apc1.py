from elasticsearch import Elasticsearch
import json
import datetime
import calendar


class ProductAnalyse(object):
    def __init__(self, payload):
        self.payload = payload

    def get_query(self):
        return {"query": BehaviorParser(self.payload).parser()}

    def analyse(self):
        query = self.get_query()
        #query["aggs"] = {}
        es = Elasticsearch(hosts='10.20.1.91', port=9200)
        return es.search(index="anal_product_cat_2020-02", body=query, size=0)


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
        gpsid = json.load(f)["gpsid"]
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
    query = ProductAnalyse(payload).get_query()
    with open("query.json", "w", encoding='utf-8') as f:
        f.write(json.dumps(query, indent=4, ensure_ascii=False))
    data = ProductAnalyse(payload).analyse()
    with open("data.json", "w", encoding='utf-8') as f:
        f.write(json.dumps(data, indent=4, ensure_ascii=False))
