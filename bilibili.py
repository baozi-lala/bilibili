import pymysql
import threading
import requests
import pprint

mylock=threading.RLock()
try:
    conn=pymysql.connect(host='localhost',user='root',passwd='mysql1820',db='bilibili')
except  Exception as err:
    print(str(err))
    exit(0)

count=6784200
MAX_COUNT=6784220
def insert(db,number,data):
    try:
        if len(set(data.values()))==1:
            return 0
    except:
        return 0
    # print(data.values())
    for each in data.values():
        if type(each)!=type(1):
            return 0

    cursor = db.cursor()
    sql="insert into v_score values("+\
        str(number)+','+\
        str(data['view']) + ','+ \
        str(data['danmaku'])+','+ \
        str(data['reply']) + ',' + \
        str(data['favorite'])+','+ \
        str(data['coin']) + ',' + \
        str(data['share'])+','+ \
        str(data['now_rank']) + ',' + \
        str(data['his_rank']) + ',' + \
        str(data['like']) + ',' + \
        str(data['dislike'])+')'
    print(number, "  ", sql)
    try:

        cursor.execute(sql)
        db.commit()
    except:
        print('it is failed to insert')
        db.rollback()
    cursor.close()

class getbilibili_vedioinf(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
    def run(self):
        global count
        global conn
        while True:
            mylock.acquire()
            number = count

            count += 1

            mylock.release()

            if number >= MAX_COUNT:
                break
            # mylock.acquire()
            # if isExistAvId(conn, number):
            #     print('提示：数据库已经存在该视频')
            #     mylock.release()
            #     continue
            # mylock.release()
            url = "http://api.bilibili.com/archive_stat/stat?aid=" + str(number)
            resp = requests.get(url)
            print(resp)
            try:
                data = resp.json()
            except:
                print(number, '  invalid json!')
                continue

            temp = data['data']
            mylock.acquire()
            insert(conn, number, temp)
            mylock.release()


# 返回视频是否已经存在数据库
def isExistAvId(db,number):
    cursor=db.cursor()
    try:
        cursor.execute('select aid from v_score where aid = {}'.format(number))
        result = cursor.fetchone()
    except:
        print('it is failed to select')
        db.rollback()
    cursor.close()
    if result == None:
        return False
    else:
        return True
if __name__ == '__main__':

    threads=[]
    for i in range(2):
        thread=getbilibili_vedioinf()
        threads.append(thread)
        thread.start()
    print('The task has completed!')
