from datetime import timedelta


page_height = 1030
show_all_edge = True
default_table = "orders"
accuracy = 0.0001
concurrency = 0.01
integer = 1
page_split = [1,2]

timefstr = {}
timefstr['long'] = "%Y-%m-%d %H:%M:%S"
timefstr['short'] = "%Y-%m-%d"


time = {}
time['format']={}
time['format']['short'] = "YYYY-MM-DD"
time['format']['long'] = "YYYY-MM-DD HH:mm:ss"

time['step']={}
time['step']['second'] = timedelta(seconds=1)
time['step']['minute'] = timedelta(minutes=1)
time['step']['hour'] = timedelta(hours=1)
time['step']['day'] = timedelta(days=1)
