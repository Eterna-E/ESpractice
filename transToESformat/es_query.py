def term_query(k, v):
    query = {
        "term": {
            k: v
        }
    }
    return query

def filter_query(v):
    query = {
        "bool": {
            "filter": v
        }
    }
    return query
