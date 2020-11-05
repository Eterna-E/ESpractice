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

def time_term_query(start_dt,end_dt,time_type):
    time_term = []
    time_term.append(term_query("start_dt",start_dt))
    time_term.append(term_query("end_dt",end_dt))
    time_term.append(term_query("time_type",time_type))
    return time_term