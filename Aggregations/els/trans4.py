import json
import datetime
import calendar
import os


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
                term_terms.append(
                    {condition: {key.replace("label", "lv"): label}})
            term_terms = term_terms[0] if len(term_terms) == 1 else term_terms
            labels.append({"bool": {"filter": term_terms}})
        parsed_label = {"bool": {"should": labels}}
        return parsed_label


class TimeParser(object):
    def parser(self, time_settings):
        date = list(map(lambda x: x.replace("/", "-"),
                        time_settings.get("range").split("-")))
        self.ranges = list()
        self.start_day = datetime.date.fromisoformat(date[0])
        self.end_day = datetime.date.fromisoformat(date[1])
        self.range_parser()
        self.ranges = self.ranges[0] if len(self.ranges) == 1 else self.ranges
        ranges = {"bool": {"should": self.ranges}}
        times = int(time_settings.get("times").replace("次", ""))
        times = {"term": {"times": times}}
        parsed_time = {"bool": {"filter": [ranges, times]}}
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

    def splice_packer(self, end_day, date_type):
        start_date = {"term": {"start_date": self.start_day.isoformat()}}
        end_date = {"term": {"end_date": end_day.isoformat()}}
        date_type = {"term": {"date_type": date_type}}
        self.ranges.append(
            {"bool": {"filter": [start_date, end_date, date_type]}})


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
            "range": "2019/10/26-2020/03/29",
            "times": "1次"
        }
    }

    query = BehaviorParser().parser(payload)
    a = os.system("cls")
    print(json.dumps(query, indent=4, ensure_ascii=False))
