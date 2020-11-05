# 轉換文字格式(payload)為ES搜尋格式
from es_query import *
import json
import datetime


class ESquery(object):

    def content_parser(self, payload):
        datasource = {
            "bool": {
                "filter": {
                    "term": {
                        "source": payload.get("datasource")
                    }
                }
            }
        }
        label_query = self.label_parser(payload.get("label_setting"))
        time_parser = self.time_parser(payload.get("time_setting"))
        query = {"bool": {"filter": [time_parser, label_query, datasource]}}
        return query

    def time_parser(self, time_payload):
        time_query = {
            "bool": {
                "should": []
            }
        }
        start_dt, end_dt = time_payload.get("range").split('-')
        weeks = self.date_to_week(start_dt, end_dt)

        date_list = []
        for i, week in enumerate(weeks):
            # print(i,week)
            if i == 0 or len(week) == 7:
                first_date_month = datetime.datetime.strptime(
                    week[0], '%Y-%m-%d').month
                week_diff = False
                for i, day in enumerate(week):
                    if datetime.datetime.strptime(day, '%Y-%m-%d').month != first_date_month:
                        week_diff = True
                        week_part1 = week[:i]
                        week_part2 = week[i:]
                        date_list.append((week_part1[0], week_part1[-1], 'w'))
                        date_list.append((week_part2[0], week_part2[-1], 'w'))
                        break
                if not week_diff:
                    date_list.append((week[0], week[-1], 'w'))

            elif i == len(weeks)-1 and len(week) != 7 and len(weeks) != 1:
                for day in week:
                    date_list.append((day, day, 'd'))
        print(date_list)

        time_terms = []
        for start_day, end_day, time_type in date_list:
            time_terms.append(time_term_query(start_day, end_day, time_type))

        filter_time_term_query = []
        for blft in time_terms:
            filter_time_term_query.append(filter_query(blft))
        time_query["bool"]["should"] = filter_time_term_query

        return time_query

    def label_parser(self, labels):
        label_query = {
            "bool": {
                "should": []
            }
        }
        lv_query = None
        for label in labels:
            main = label.get("main_label")
            label1, label2 = main.get("label1"), main.get("label2")
            if label1:
                lv_query = None
                if label2:
                    label1, label2 = term_query(
                        "label1", label1), term_query("label2", label2)
                    lv_query = filter_query([label1, label2])
                else:
                    lv_query = term_query("label1", label1)
            label_query["bool"]["should"].append(lv_query)

        return label_query

    def date_to_week(self, start_date, end_date):
        # 开始时间
        # start_date = '2020-10-01'
        # 结束时间
        # end_date = '2020-10-13'
        date_time1 = datetime.datetime.strptime(end_date, '%Y/%m/%d')  # 结束时间
        date_time0 = datetime.datetime.strptime(start_date, '%Y/%m/%d')  # 开始时间
        d = (date_time1 - date_time0).days + 1
        ls_date = []
        for i in range(d):  # 每一轮循环统计一天或者一周或者所有的
            date_time = date_time0 + datetime.timedelta(days=i)  # 根据i调整天数
            ls_date.append(str(date_time)[:10])
            # ls_date.append(str(date_time)) # '2018-05-04 00:00:00'

        unit_num = []
        date_time00 = date_time0.strftime('%w')  # 开始时间
        if int(date_time00) == 0:
            date_time00 = 7
        a = 7 - int(date_time00)
        unit_num.append(ls_date[:a])  # ls_date为所有日期的集合
        ls_date = ls_date[a:]
        for i in range(0, len(ls_date), 7):
            unit_num.append(ls_date[i:i + 7])

        for i, week in enumerate(unit_num):
            if len(week) == 0:
                del unit_num[i]

        return unit_num


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
            "range": "2020/09/28-2020/10/13",
            "times": "1次"
        }
    }

    es_query = ESquery().content_parser(payload)

    print(es_query)

    jsonStr = json.dumps(es_query, indent=1)
    # print(jsonStr)
    with open('output.json', 'w') as f:
        f.write(jsonStr)
