import datetime

def date_to_week(start_date,end_date):
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

    return unit_num
    # print(unit_num)

    # for i,week in enumerate(unit_num):
    #     print(i,week)
        # if start_date in week:
        #     # print(i)
        #     print('list size:',len(week))
        #     print(week.index(start_date))
        # if end_date in week:
        #     # print(i)
        #     print('list size:',len(week))
        #     print(week.index(end_date)) 

if __name__ == "__main__":
    date_to_week('2020-10-01','2020-10-13')