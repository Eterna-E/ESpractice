import json

class BehaviorParser(object):
    def parser(self, payload):
        parser_query = list()
        datasource = payload.get("datasource")
        label_query = self.label_parser(payload.get("label_setting"))
        time_query = self.time_parser(payload.get("time_setting"))
        return parser_query

    def label_parser(self, label_settings):
        for label_setting in label_settings:
            pass


    def time_parser(self, time_settings):
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
    #            "brands": []
            },
            {
                "main_label": {
                    "label1": "服裝與飾品"
                },
    #            "brands": []
            }
        ],
        "time_setting": {
            "range": "2020/01/27-2020/02/02",
            "times": "1次"
        }
    }
    query = BehaviorParser().parser(payload)
    print(query)