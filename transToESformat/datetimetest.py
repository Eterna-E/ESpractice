import datetime

# print(datetime.date(2020,11,4).isocalendar()[1])
# print(datetime.date(2020,11,5).isocalendar()[1])
# print(datetime.date(2020,11,6).isocalendar()[1])

def get_week():
    today = datetime.date.today()
    month = today.month
    year = today.year
    day = today.day
    weekday = today.weekday()
    
    start = today + datetime.timedelta(-1-weekday)
    end = today + datetime.timedelta(5-weekday)
    
    start = datetime.datetime(start.year,start.month,start.day)
    end = datetime.datetime(end.year,end.month,end.day)
    
    return start, end

# print(get_week())

def all_weeks(year=2015):
    '''计算一年内所有周的具体日期,每周都是7天，可能最后一周到 下年
     week_date 输出如{1: ['20181231', '20190101', '20190102', '20190103', '20190104', '20190105', '20190106'],...}
     计算一年内所有周的起始日期
     week_date_start_end {1: ['20181231','20190106'],...}
     '''
 
    start_date=datetime.datetime.strptime(str(int(year)-1)+'1224','%Y%m%d')
    end_date=datetime.datetime.strptime(str(int(year)+1)+'0107','%Y%m%d')
    _u=datetime.timedelta(days=1)
    n=0
    week_date={}
    while 1:
        _time=start_date+n*_u
        y,w=_time.isocalendar()[:2]
        if y==year :
            week_date.setdefault(w,[]).append(_time.strftime('%Y%m%d'))
        n=n+1
        if _time==end_date:
            break
    week_date_start_end={}
    for i in week_date:
       week_date_start_end[i]=[week_date[i][0],week_date[i][-1]]
    print(week_date)
    # print week_date_start_end
    return week_date,week_date_start_end

# all_weeks()
# 开始时间
start_date = '2020-10-01'
# 结束时间
end_date = '2020-10-13'
date_time1 = datetime.datetime.strptime(end_date, '%Y-%m-%d')  # 结束时间
date_time0 = datetime.datetime.strptime(start_date, '%Y-%m-%d')  # 开始时间
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

# print(unit_num)

for i,week in enumerate(unit_num):
    print(i,week)
    if start_date in week:
        # print(i)
        print('list size:',len(week))
        print(week.index(start_date))
    if end_date in week:
        # print(i)
        print('list size:',len(week))
        print(week.index(end_date))