{
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
            "range": "2020/01/27-2020/02/02",
            "times": "1次"
        }
    }



    {
  "bool": {
    "filter": [
      {
        "bool": {
          "filter": [
            {
              "range": {
                "date": {
                  "gte": "2020-01-27",
                  "lte": "2020-02-02"
                }
              }
            },
            {
              "term": {
                "time": 1
              }
            }
          ]
        }
      },
      {
        "bool": {
          "should": [
            {
              "bool": {
                "filter": [
                  {
                    "term": {
                      "lv1": "社群"
                    }
                  },
                  {
                    "terms": {
                      "lv2": [
                        "論壇"
                      ]
                    }
                  }
                ]
              }
            },
            {
              "bool": {
                "filter": {
                  "term": {
                    "lv1": "服裝與飾品"
                  }
                }
              }
            }
          ]
        }
      },
      {
        "bool": {
          "filter": {
            "term": {
              "source": "Behavior Tree"
            }
          }
        }
      }
    ]
  }
}