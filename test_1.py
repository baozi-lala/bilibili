# coding: utf-8
# Writer: bao
# Date: 2018-11-22

import json
import requests
import re
import pymysql
import time

try:
    conn = pymysql.connect(host='localhost', user='root', passwd='mysql1820', db='bilibili', use_unicode=True,
                           charset="utf8")
except  Exception as err:
    print(str(err))
    exit(0)
def create(table_name):
    # 使用cursor()方法获取操作游标
    cursor = conn.cursor()
    # 如果数据表已经存在使用 execute() 方法删除表。
    cursor.execute("DROP TABLE IF EXISTS %s"%(table_name))
    # 创建数据表SQL语句
    sql = """CREATE TABLE user_based_info (
             aid  int NOT NULL PRIMARY KEY,
             title  varchar(255),
             subtitle varchar(255),
             description varchar(255),
             author varchar(255), 
             mid  int,
             view int,  
             danmaku int,
             reply int,
             favorite int,
             coin int,  
             share int,
             now_rank int,
             his_rank int,  
             likes int,
             dislikes int,
             length varchar(255),
             created varchar(255),
             pic varchar(255))ENGINE=innodb DEFAULT CHARSET=utf8;"""
    try:
        cursor.execute(sql)
        conn.commit()
    except:
        print('it is failed to create')
        conn.rollback()
    cursor.close()

def insert(db,data):
    url = "http://api.bilibili.com/archive_stat/stat?aid=" + str(data['aid'])
    resp = requests.get(url)
    data_api = resp.json()
    temp = data_api['data']
    timeArray = time.localtime(data['created'])
    otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray )
    data['created']=otherStyleTime
    cursor = db.cursor()
    sql="replace into user_based_info values(%s,'%s','%s','%s','%s',%s," \
        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
        "'%s','%s','%s')"%\
        (data['aid'], data['title'].strip(),data['subtitle'],data['description'].strip(),data['author'],data['mid'],
         temp['view'],temp['danmaku'],temp['reply'],temp['favorite'],temp['coin'],
         temp['share'],temp['now_rank'],temp['his_rank'],temp['like'],temp['dislike'],
         data['length'],data['created'],data['pic'])
    # data['play'],data['comment'],data['review'],data['video_review'],data['is_pay'],data['favorites'],
    # print(sql)
    try:
        cursor.execute(sql)
        db.commit()
    except:
        print('it is failed to insert')
        db.rollback()
    cursor.close()


def getbilibili_vedioinf(users_url):
    try:
        for mid in users_url:
            print(mid)
            page = 1
            while True:
                url_vlist = "https://space.bilibili.com/ajax/member/getSubmitVideos?mid=%s&pagesize=100&tid=0&page=%s&keyword=&order=pubdate" % (
                mid, page)
                # print url_vlist
                response = requests.get(url_vlist, verify=False)
                vlist = response.json()
                for info in vlist['data']['vlist']:
                    insert(conn, info)
                    # print info['aid']
                if vlist['data']['pages'] > page:
                    page = page + 1
                else:
                    break
    except:
        print('no found')





if __name__ == "__main__":
    # url = 'https://space.bilibili.com/ajax/member/getSubmitVideos?mid=20165629&pagesize=100&tid=0&page=1&keyword=&order=pubdate'
    # 创建数据库
    table_name = "user_based_info"
    create(table_name)
    # 21778636 中国日报
    # 20165629  共青团中央
    # 10303206  环球时报
    # 11464329  光明网
    # 10330740  观察者网
    # 107255471 CCTV2第一时间 官方账号
    # 258844831 CCTV_国家宝藏
    # 274900004 CCTV4
    url_list=[21778636,20165629,10303206,107255471,258844831,274900004]
    # space_url = "https://space.bilibili.com/21778636/#/video?tid=0&page=1&keyword=&order=pubdate"
    getbilibili_vedioinf(url_list)
    print('The task has completed!')
    # # 下面开始下载
    # for i in range(len(Video_List)):
    #     download(i,Video_List,Get_Path(Video_List))
