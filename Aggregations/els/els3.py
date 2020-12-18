from elasticsearch import Elasticsearch

def load_datas():
    datas = list()
    datas.append(
        {
            "sid": 1090101,
            "name": "nmsl",
            "age": 200,
            "class": "dddd"
        }
    )
    datas.append(
        {
            "tid": 11090101,
            "name": "cnmd",
            "age": 250,
            "class": "aaa",
            "salary": 35000
        }
    )
    return datas

def create_data(es, datas):
    for data in datas:
        es.index(index='school2', body=data)  

if __name__ == "__main__":
    es = Elasticsearch(hosts='192.168.1.59', port=9200)
    datas = load_datas()
    create_data(es, datas)