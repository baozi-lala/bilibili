# coding: utf-8
# Writer: bao
# Date: 2018-11-30

import json
import requests
import re
import pymysql
from bs4 import BeautifulSoup
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
    sql = """CREATE TABLE comment_db (
             aid  int NOT NULL PRIMARY KEY,
             comment varchar(255),
             floor int,
             rcount int,
             likes int,
             uname varchar(255),
             mid int,
             sex varchar(255),
             sign varchar(255),
             ctime varchar(255))ENGINE=innodb DEFAULT CHARSET=utf8;"""
    try:
        cursor.execute(sql)
        conn.commit()
    except:
        print('it is failed to create')
        conn.rollback()
    cursor.close()

def insert_to_comment_db(db,aid):
    try:
        comment = 1
        page = 1
        while True:
            url = "https://api.bilibili.com/x/v2/reply?&pn=%s&type=1&oid=%s" % \
              (page, aid)
            resp = requests.get(url)
            data_api = resp.json()
            temp = data_api['data']
            replies = temp['replies']
            for reply in replies:
                reply['ctime'] =time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(reply['ctime']) )
                cursor = db.cursor()
                sql = "replaceintocomment_dbvalues(%s,'%s',%s,%s,%s,'%s'," \
                  "%s,'%s','%s'" \
                  "'%s')" % \
                  (aid, reply['content']['message'], reply['floor'], reply['rcount'], reply['like'],
                   reply['member']['uname'],
                   reply['member']['mid'], reply['member']['sex'], reply['member']['sign'],
                   reply['ctime'])
                print(sql)
                try:
                    cursor.execute(sql)
                    db.commit()
                    comment = comment + 1
                except:
                    print('it is failed to insert')
                    db.rollback()
                cursor.close()
            if temp['page']['acount'] > comment:
                page = page + 1
            else:
                break
    except:
        print("error")



def getbilibili_vedioinf(keyword_list):

    try:
        for mid in keyword_list:
            page = 1
            while True:
                url_vlist = "https://search.bilibili.com/all?keyword=%s&page=%s" % (
                mid, page)
                # print url_vlist
                response = requests.get(url_vlist)
                html_doc = response.text
                soup=BeautifulSoup(html_doc,'lxml')
                # print(html_doc)
                divs = soup.find_all('li',attrs={"class": "video matrix"})
                if divs:
                    for div in divs:
                        pic=div.find("img").get('src')
                        length=div.find("span",attrs={"class": "so-imgTag_rb"}).get_text()
                        avid=div.find(class_="type avid").get_text()
                        type=div.find(class_="type hide").get_text()
                        info=div.find("a", attrs={"class": "title"})
                        title = info.get('title')
                        url = info.get('href')
                        des=div.find("div", attrs={"class": "des hide"}).get_text()
                        tags = div.find("div", attrs={"class": "tags"})
                        time = tags.find("span", attrs={"class": "so-icon time"}).get_text()
                        author = tags.find("a", attrs={"class": "up-name"}).get_text()
                        data=dict(aid=avid[2:],url=url,title = title,description=des.strip(),
                                  type=type.strip(), author=author,created =time.strip(),
                                  length=length.strip(), pic=pic.strip())
                        insert(conn, data)
                    page = page + 1
                else:
                    break
    except:
        print('no found')


if __name__ == "__main__":
    # url = 'https://space.bilibili.com/ajax/member/getSubmitVideos?mid=20165629&pagesize=100&tid=0&page=1&keyword=&order=pubdate'
    # 创建数据库
    table_name = "keyword_based_info"
    create(table_name)
    # keyword:
    # 20165629  共青团中央
    # 10303206  环球时报
    # 107255471 CCTV2第一时间 官方账号
    # 258844831 CCTV_国家宝藏
    # 274900004 CCTV4
    keyword_list=["人民日报","新华社","新华网","中国新闻社","CCTV","人民网","人民日报","环球时报","共青团中央"]
    # space_url = "https://space.bilibili.com/21778636/#/video?tid=0&page=1&keyword=&order=pubdate"
    getbilibili_vedioinf(keyword_list)

