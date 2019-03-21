'''
利用多进程方式采集房源数据
'''

from urllib import request, error, response
from bs4 import BeautifulSoup
import json
from pandas import DataFrame
import random
import re
from multiprocessing import Process, Pool
import csv
import time


def getHeader():
    hds = [
        {'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'},
        {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 Safari/535.11'},
        {'User-Agent': 'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)'},
        {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0'},
        {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/44.0.2403.89 Chrome/44.0.2403.89 Safari/537.36'},
        {
            'User-Agent': 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},
        {
            'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},
        {'User-Agent': 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0'},
        {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},
        {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},
        {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'},
        {'User-Agent': 'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11'},
        {'User-Agent': 'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11'}]

    header = hds[random.randint(0, len(hds) - 1)]
    return header


def getContent(url):
    head = getHeader()

    try:
        req = request.Request(url, headers=head)
        res = request.urlopen(req, timeout=10)

        content = res.read().decode("utf-8")
        return content
    except error.HTTPError as e:
        print(e.reason)
    except error.URLError as e:
        print(e.reason)
    except Exception as e:
        print(e)


def getPage(soup):
    try:
        pagedict = soup.find('div', {'class': 'page-box house-lst-page-box'}).get('page-data')
        # exec(d)
        pagedict = pagedict.strip("\\").strip("'").strip("\\")
        pagedict.replace("\\", "")
        pagedict.replace("'", "")
        pagedict = json.loads(pagedict, encoding="utf-8")
        totalpage = pagedict['totalPage']
        totalpage = int(totalpage)
        return totalpage
    except Exception as e:
        print(e)


def doHtml(soup, i):
    houses = soup.select(".sellListContent li")
    sources = []
    try:
        for item in houses:  # .select("li"):
            house = item.select(".houseInfo")
            position = item.select(".positionInfo")
            addr = item.select(".positionInfo a")
            taxf = item.select(".taxfree")
            totalpri = item.select(".totalPrice")
            unitpri = item.select(".unitPrice")

            h, p, a, t, total, unit = "", "", "", "", "", ""
            if len(house) > 0:
                h = house[0].text.split("|")
            if len(position) > 0:
                p = position[0].text
            if len(addr):
                a = addr[0].text

            if len(taxf) > 0:
                t = taxf[0].text
            if len(totalpri) > 0:
                total = totalpri[0].text
            if len(unitpri) > 0:
                unit = unitpri[0].text.replace("单价", "")

            isHasDianti = ""
            if len(h) > 5:
                isHasDianti = h[5]
            year = re.findall(r"\d{4}年", p)
            if len(year) > 0:
                year = year[0]
            totalfloor = re.findall(r"共\d{1,3}层", p)
            if len(totalfloor) > 0:
                totalfloor = totalfloor[0]
            buildType = re.findall(r"\d{4}年建(\w{2,})", p)
            if len(buildType) > 0:
                buildType = buildType[0]
            floorType = re.findall(r"(\w{2,})[(]共", p)
            if len(floorType) > 0:
                floorType = floorType[0]

            source = {}
            source.update({u'小区名称': h[0]})
            source.update({u'户型': h[1]})
            source.update({u'面积': h[2]})
            source.update({u'朝向': h[3]})
            source.update({u'有电梯': isHasDianti})
            source.update({u'楼层类型': floorType})
            source.update({u'建筑结构': buildType})
            source.update({u'地址': a})
            source.update({u'总楼层': totalfloor})
            source.update({u'年份': year})
            source.update({u'房本情况': t})
            source.update({u'总价': total})
            source.update({u'单价': unit})
            source.update({u'年份': year})
            sources.append(source)
        tocsv(sources)
        i += 1
    except Exception as e:
        print(e)


def house_spider(url, i):
    content = getContent(url)
    html_soup = BeautifulSoup(content, 'html.parser')
    doHtml(html_soup, i)
    print("完成第 %s 页" % (str(i)))
    return html_soup


# def listtocsv(list, out_file, is_first):
#     with open(out_file, 'a') as f:
#         w = csv.writer(f)
#         if is_first:
#             fieldnames = list[0].keys()
#             w.writerow(fieldnames)
#         for row in list:
#             w.writerow(row.values())


def tocsv(data):
    df = DataFrame(data=data)
    df.to_csv("fangyuan_mulprocess.csv", mode="a")


if __name__ == '__main__':
    starttime = time.time()
    url = "https://lf.lia_jia.com/ershoufang/yanjiao/pg1/"
    html_soup = house_spider(url, 1)
    pageInfo = html_soup.select(".house-lst-page-box[page-data]")
    if len(pageInfo) > 0:
        jsonStr = pageInfo[0]
    p = Pool(4)
    totalpage = getPage(html_soup)

    for i in range(2, totalpage):
        url = "https://lf.lia_jia.com/ershoufang/yanjiao/pg" + str(i) + "/"
        p.apply_async(house_spider, args=(url, i))

    p.close()
    p.join()
    endtime = time.time()

    print("采集完成，共消耗时间:%s" % (str(endtime - starttime)))
