# coding: utf-8
# Writer: bao
# Date: 2018-11-30

import json
import requests
import re
import pymysql
from bs4 import BeautifulSoup
import time
import sys
from you_get import common as you_get #导入you-get库
import os
try:
    conn = pymysql.connect(host='localhost', user='root', passwd='mysql1820', db='bilibili', use_unicode=True,
                           charset="utf8mb4")
    # cursor.execute("SET NAMES utf8mb4;")
except  Exception as err:
    print(str(err))
    exit(0)

def create_keyword_db(table_name):
    # 使用cursor()方法获取操作游标
    cursor = conn.cursor()
    # 如果数据表已经存在使用 execute() 方法删除表。
    cursor.execute("DROP TABLE IF EXISTS %s"%(table_name))
    # 创建关键字数据表SQL语句
    sql = """CREATE TABLE keyword_db (
             aid  int NOT NULL PRIMARY KEY,
             title  varchar(1000),
             description varchar(1000),
             type varchar(255),
             tags varchar(255),
             author varchar(255), 
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
             pic varchar(1000),
             path varchar(1000))ENGINE=innodb DEFAULT CHARSET=utf8mb4;"""
    try:
        cursor.execute(sql)
        conn.commit()
    except:
        print('it is failed to create')
        conn.rollback()
    cursor.close()
def create_comment_db(table_name):
    # 使用cursor()方法获取操作游标
    cursor = conn.cursor()
    # 如果数据表已经存在使用 execute() 方法删除表。
    cursor.execute("DROP TABLE IF EXISTS %s"%(table_name))
    # 创建评论数据表SQL语句
    sql_comment = """CREATE TABLE comment_db (
             aid  int,
             comment varchar(1000),
             floor int,
             rcount int,
             likes int,
             uname varchar(255),
             mid int,
             sex varchar(255),
             sign varchar(255),
             ctime varchar(255))ENGINE=innodb DEFAULT CHARSET=utf8mb4;"""
    try:
        cursor.execute(sql_comment)
        conn.commit()
    except:
        print('it is failed to create comment_db')
        conn.rollback()
    cursor.close()

def create_danmu_db(table_name):
    # 使用cursor()方法获取操作游标
    cursor = conn.cursor()
    # 如果数据表已经存在使用 execute() 方法删除表。
    cursor.execute("DROP TABLE IF EXISTS %s"%(table_name))
    # 创建评论数据表SQL语句
    sql_comment = """CREATE TABLE danmu_db (
             aid  int,
             cid int,
             danmu varchar(255))ENGINE=innodb DEFAULT CHARSET=utf8mb4;"""
    try:
        cursor.execute(sql_comment)
        conn.commit()
    except:
        print('it is failed to create danmu_db')
        conn.rollback()
    cursor.close()

def insert(db,data):
    # url = "http"+ str(data['url'])
    # url='https://www.bilibili.com/video/{}'.format(data['avid'])
    url = "http://api.bilibili.com/archive_stat/stat?aid=" + str(data['aid'])
    resp = requests.get(url)
    data_api = resp.json()
    temp = data_api['data']
    cursor = db.cursor()
    sql="replace into keyword_db values(%s,'%s','%s','%s','%s','%s'," \
        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
        "'%s','%s','%s','%s')"%\
        (data['aid'], data['title'],data['description'],data['type'],data['tags'],data['author'],
         temp['view'],temp['danmaku'],temp['reply'],temp['favorite'],temp['coin'],
         temp['share'],temp['now_rank'],temp['his_rank'],temp['like'],temp['dislike'],
         data['length'],data['created'],data['pic'],data['path'])
    # data['play'],data['comment'],data['review'],data['video_review'],data['is_pay'],data['favorites'],
    print(sql)
    try:
        cursor.execute(sql)
        db.commit()
    except:
        print('it is failed to insert')
        db.rollback()
    cursor.close()

    print("评论", id)
    print("----------------")
    insert_to_comment_db(db, data['aid'])
    print("评论end:","----------------")
    print('\n弹幕', "----------------")
    insert_to_danmu_db(db, data['aid'])
    print("弹幕end:", "----------------")

def insert_to_comment_db(db,aid):
    # try:
    comment = 1
    page = 1
    while True:
        print("comment_page:",page)
        url = "https://api.bilibili.com/x/v2/reply?&pn=%s&type=1&oid=%s" % \
          (page, aid)
        resp = requests.get(url)
        data_api = resp.json()
        if data_api['code'] != 0:
            break
        temp = data_api['data']
        replies = temp['replies']
        if not replies:
            break
        for reply in replies:
            if not reply:
                break
            reply['ctime'] =time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(reply['ctime']) )
            cursor = db.cursor()
            sql = "replace into comment_db values(%s,'%s',%s,%s,%s," \
              "'%s',%s,'%s','%s'," \
              "'%s')" % \
              (aid, reply['content']['message'], reply['floor'], reply['rcount'], reply['like'],
               reply['member']['uname'],reply['member']['mid'], reply['member']['sex'], reply['member']['sign'],
               reply['ctime'])
            print(sql)
            comment = comment + 1
            try:
                cursor.execute(sql)
                db.commit()

            except:
                print('it is failed to insert to comment_db')
                db.rollback()
            cursor.close()
        if temp['page']['count'] > comment:
            page = page + 1
        else:
            break


def insert_to_danmu_db(db,aid):
    while True:
        url = "https://api.bilibili.com/x/web-interface/view?aid=%s" % \
              (aid)
        resp = requests.get(url)
        data_api = resp.json()
        cid = data_api['data']['cid']
        url = "https://comment.bilibili.com/%s.xml" % (cid)
        html = requests.get(url).content
        soup = BeautifulSoup(html, "lxml")
        results = soup.find_all('d')
        cursor = db.cursor()
        for x in results:
            sql = "insert into danmu_db values(%s,%s,'%s')" % \
                  (aid, cid, x.get_text())
            print(sql)
            try:
                cursor.execute(sql)
                db.commit()
            except:
                print('it is failed to insert to danmu_db')
                db.rollback()
        cursor.close()

def getbilibili_vedioinf(keyword_list):
    #
    # try:
    for mid in keyword_list:
        print("--------------------------")
        print("keyword:",mid)
        print("--------------------------")
        # 创建下载目录
        directory = r'D:/video/' + mid + "/"
        if not os.path.isdir(directory):
            os.makedirs(directory)
        page = 1
        while True:
            print("\nvideo_page:",page)
            url_vlist = "https://api.bilibili.com/x/web-interface/search/type?&&search_type=video&highlight=1&keyword=%s&page=%s" % (
            mid, page)
            response = requests.get(url_vlist)
            data = response.json()
            result = data['data']['result']
            for r in result:
                pic = r['pic']
                id = r['id']
                length = r['duration']
                type = r['typename']
                # title = r['title']s
                title = re.sub('<em class="keyword">', '', r['title'])
                title = re.sub('</em>', '', title)
                des = r['description']
                tags = r['tag']
                times =time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(r['senddate']) )
                author = r['author']
                # download(directory, id)
                download_path=os.path.join(directory,title+'.flv')
                print("----------------")
                print("视频av",id)
                print("----------------")
                info = dict(aid=id, title=title, description=des.strip(), type=type.strip(),tags=tags,
                            author=author, created=times.strip(), length=length.strip(), pic=pic.strip(),
                            path=download_path)
                insert(conn, info)
                print("视频end","----------------\n")
            page = page + 1
            if page > data['data']['numPages']:
                break

    # except:
    #     print('no found')

# 下载视频
def download(directory,aid):
    url = 'https://www.bilibili.com/video/av' + str(aid) + '/'  # 需要下载的视频url
    sys.argv = ['you-get', '-o', directory, url]  # sys传递参数执行下载，就像在命令行一样
    you_get.main()

if __name__ == "__main__":
    # url = 'https://space.bilibili.com/ajax/member/getSubmitVideos?mid=20165629&pagesize=100&tid=0&page=1&keyword=&order=pubdate'
    # 创建数据库
    table_name = "keyword_db"
    create_keyword_db(table_name)
    create_comment_db("comment_db")
    create_danmu_db("danmu_db")
    # keyword:
    # 20165629  共青团中央
    # 10303206  环球时报
    # 107255471 CCTV2第一时间 官方账号
    # 258844831 CCTV_国家宝藏
    # 274900004 CCTV4
    keyword_list=["人民日报","新华社","新华网","中国新闻社","CCTV","人民网","中国日报","环球时报","共青团中央"]
    # space_url = "https://space.bilibili.com/21778636/#/video?tid=0&page=1&keyword=&order=pubdate"
    getbilibili_vedioinf(keyword_list)

