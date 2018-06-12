# coding=utf-8
import csv
import calendar
import datetime
import json
import random
import re

import requests

from janome.tokenizer import Tokenizer
from google.cloud import bigquery
from google.oauth2 import service_account

json_key = 'continual-grin-206507-54b15b168106.json'
client = bigquery.Client.from_service_account_json(json_key, project='continual-grin-206507')

player_record = {}
months = {}
for i, v in enumerate(calendar.month_abbr):
    months[v] = i


class SportsLive:
    def __init__(self, parent=None):
        pass

    '''
    形態素解析
    '''
    @staticmethod
    def morphological_analysis(text):
        txt = text
        t = Tokenizer()
        word_dic = {}
        lines = txt.split("\r\n")
        for line in lines:
            blog_txt = t.tokenize(line)
            for w in blog_txt:
                word = w.surface
                ps = w.part_of_speech
                if ps.find('名詞') < 0:
                    continue
                if word not in word_dic:
                    word_dic[word] = 0
                word_dic[word] += 1

        keys = sorted(word_dic.items(), key=lambda x: x[1], reverse=True)
        keyword = ''
        for word, cnt in keys[:4]:
            print("{0} ".format(word))
            keyword += "{0} ".format(word)

        return keyword

    """v1 direct score getting"""
    def score_check(self, keyword):
        data = []

        try:
            target_url = 'https://sports.yahoo.co.jp/search/text?query=' + keyword
            resp = requests.get(target_url)
            soup = BeautifulSoup(resp.text, "html.parser")

            tables = soup.find_all("p", class_="siteUrl")

            for table in tables:
                geturl = table.text
                geturl = geturl.rstrip(' － キャッシュ')

                data.append(geturl)
        except:
            pass
        score = ''

        try:
            for url in data:
                if 'game' in url:
                    score = self.get_score(url)
                    break
                else:
                    continue

        except:
            pass

        return score

    """v2 news server loading"""
    def news_loader(self, keyword, rowcount, day, debug=False):
        myquery = ""
        news_dict = {}
        output_text = ""
        rowcount_str = ""
        json_dict = {}         

        if 1 <= rowcount < 5:
            rowcount_str = "row{}_text".format(str(rowcount))
        else:
            rowcount_str = "Full_text"

        if debug and rowcount_str == "Full_text":
            myquery = """
                        SELECT Full_text as text,title,Full_text FROM sportsagent.newsrecord
                        WHERE title like '%{1}%' AND _PARTITIONTIME = TIMESTAMP('{0}')
                      """.format(day, str(keyword))
        elif debug:
            myquery = """
                        SELECT {0} as text,title,Full_text FROM sportsagent.newsrecord
                        WHERE title like '%{2}%' AND _PARTITIONTIME = TIMESTAMP('{1}')
                      """.format(rowcount_str, day, str(keyword))
        else:
            myquery = """
                        SELECT title as text, {0} FROM sportsagent.newsrecord
                        WHERE title like '%{2}%' AND _PARTITIONTIME = TIMESTAMP('{1}')
                      """.format(rowcount_str, day, str(keyword))
        try:
            query_job = client.query(myquery)
            results = query_job.result()  # Waits for job to complete.
            result_list = list(results)
        except:
            raise NameError(myquery)
        
        try:
            if 1 <= rowcount < 5:
                # random select for results
                randindex = random.randint(0, len(result_list) - 1)
                output_text = result_list[randindex][0]
            else:
                text = "".join([re.text for re in result_list])
                output_text = self.analsys_text(text, rowcount)
        except:
            raise NameError("get errors?")

        json_dict = {"speech": output_text,
                     "displayText": output_text,
                     "source": "apiai-news"}

        return json_dict

    """v2 player server loading"""
    @staticmethod
    def player_loader(keyword, day, debug=False):
        news_dict = {}
        output_text = ""

        if debug:
            myquery = """
                        SELECT name,record as text
                        FROM sportsagent.bplayerrecord
                        WHERE name like '%{0}%'
                      """.format(str(keyword))
        else:
            myquery = """
                        SELECT name,record as text
                        FROM sportsagent.bplayerrecord
                        WHERE name like '%{0}%'
                      """.format(str(keyword))
        if day is not None:
            myquery += " AND _PARTITIONTIME = TIMESTAMP('{0}')".format(day)

        query_job = client.query(myquery)
        results = query_job.result()  # Waits for job to complete.
        result_list = list(results)
        
        output_text = str(result_list[0][0]) + "は" + str(result_list[0][1])

        json_dict = {"speech": output_text,
                     "displayText": output_text,
                     "source": "apiai-player"}

        return json_dict

    @staticmethod
    def tweet_analysis(text):
        sentences, debug_info = summarize(
            text, sent_limit=5, continuous=True, debug=True
        )

        return sentences

    @staticmethod
    def analsys_text(text, rowcount):
        sentences, debug_info = summarize(
            text, sent_limit=rowcount, continuous=True, debug=True
        )

        return sentences

    @staticmethod
    def summarized(text, rowcount):
        json_dict = {}
        sentences, debug_info = summarize(
            text, sent_limit=rowcount, continuous=True, debug=True
        )
        
        output_text = " ".join(sentences)
        json_dict.update({"result_text": output_text})
        encode_json_data = json.dumps(json_dict)

        return encode_json_data

    @staticmethod
    def execute_sql(keyword, table, keyfield, fields, debug=False, day=None):
        news_dict = {}
        output_text = ""
        
        print("day = {}".format(day))
        
        if type(fields) is list:
            field = ",".join(fields)

        myquery = """
                    SELECT {3}
                    FROM sportsagent.{1}
                    WHERE {2} like '%{0}%'
                  """.format(keyword, table, keyfield, field)
        if day is not None and day != "":
            myquery += " AND DATE = '{0}'".format(day)
        myquery +=  " ORDER BY TIME DESC"
        print(myquery)
        
        try:
            query_job = client.query(myquery)
            results = query_job.result()  # Waits for job to complete.
        except Exception as e:
            print(e.args)
               
        result_list = list(results)
        
        print(result_list)

        output_text = str(keyword) + "は" + str(result_list[0][1])

        json_dict = {"speech": output_text,
                     "displayText": output_text,
                     "source": "apiai-player"}

        return json_dict

    @staticmethod
    def execute_sql2(keywords, table, keyfields, fields, debug=False, day=None):
        news_dict = {}
        output_text = ""
        where = ""

        print("day = {}".format(day))
        
        if type(fields) is list:
            field = ",".join(fields)

        for f,k in zip(keyfields,keywords):
            print(f + ":" + k)
            where += "{0} like '%{1}%' AND ".format(f, k)
        where = where[:-4]

        myquery = """
                    SELECT {2}
                    FROM sportsagent.{1}
                    WHERE {0}
                  """.format(where, table, field)
        print(myquery)
        if day is not None and day != "":
            myquery += " AND DATE = '{0}'".format(day)
            
        myquery += " ORDER BY date,TIME DESC"
        
        print(myquery)

        try:
            query_job = client.query(myquery)
            results = query_job.result()  # Waits for job to complete.
        except Exception as e:
            print(e.args)
            raise ValueError("error!")
        
        result_list = list(results)
        print(result_list)
        print(result_list[0][0])        
        output_text = "{0}-{1}".format(str(result_list[0][0]),str(result_list[0][1])) + "は" + str(result_list[0][2]) 

        json_dict = {"speech": output_text,
                     "displayText": output_text,
                     "source": "apiai-player"}

        return json_dict


class RecordAccumulation:
    def __init__(self):
        pass

    @staticmethod
    def save_csv(table, filename):
        with open(filename, "w", encoding="utf-8", newline='') as f:
            writer = csv.writer(f)
            for row in table:
                writer.writerow(row)

    """v2 current"""
    @staticmethod
    def get_jp_bplayer_record(date):
        rec_list = [["name", "type", "date", "time", "record"]]
        rec_tuple = []

        i = 1
        strdate = date.strftime('%Y%m%d')

        while True:
            # URL構築
            req = requests.get(r_base_url_baseball[0] +
                               strdate +
                               str(i).zfill(2) +
                               r_prop[1])

            if req.status_code != 200:
                break

            try:
                soup = BeautifulSoup(req.text, "lxml")
            except:
                soup = BeautifulSoup(req.text, "html.parser")

            # バッターの成績
            try:
                tables = soup.findAll("table", class_="yjS")

                for table in tables:
                    trlist = table.findAll("tr")
                    for tr in trlist:
                        if "位" in tr.text or "合計" in tr.text:
                            continue
                        td = tr.findAll("td")

                        record = [td[1].string,
                                  "b",
                                  strdate,
                                  datetime.datetime.now().strftime('%H%M%S'),
                                  td[3].string + "打数" +
                                  td[5].string + "安打 " +
                                  td[4].string + "得点で打率は" +
                                  td[2].string + "です。"]
                        rec_list.append(record)
                        rec_tuple.append(tuple(record))
            except:
                continue

            # ピッチャーの成績
            try:
                divs = soup.findAll("div", class_="pitcher")

                for div in divs:
                    trlist = div.findAll("tr")
                    for tr in trlist:
                        if "防御率" in tr.text:
                            continue
                        td = tr.findAll("td")

                        record = [td[1].string,
                                  "p",
                                  strdate,
                                  datetime.datetime.now().strftime('%H%M%S'),
                                  td[3].string + "投球回" +
                                  td[4].string + "投球数で" +
                                  "被安打が" + td[6].string +
                                  td[8].string + "奪三振しています。" +
                                  "防御率は" + td[2].string + "です。"]
                        rec_list.append(record)
                        rec_tuple.append(tuple(record))
            except:
                continue

            i += 1

        return rec_list, rec_tuple

    """v2 current"""
    def news_check(self, date):
        pattern = r'（.*）'
        news_dict = {}
        output_text = ""
        news_list = [["title", "url", "Full_text", "row1_text", "row2_text", "row3_text", "row4_text", "time"]]
        news_tuple = []

        try:
            for rss in rss_news:
                resp = requests.get(rss)
                soup = BeautifulSoup(resp.text, "html.parser")

                items = soup.find_all("item")

                for item in items:
                    title = item.find_all("title")[0]
                    link = item.find_all("link")[0]
                    day = item.find_all("pubdate")[0].text

                    news_date = day.split(" ")
                    news_date = datetime.date(int(news_date[3]),
                                              int(months[news_date[2]]),
                                              int(news_date[1]))
                    if date == news_date:
                        news_dict.update({title.text: str(link.next).replace('\n', '').replace(' ', '')})

            news_key_list = [l for l in news_dict.keys()]
        except:
            raise NameError("get?")

        for list_key in news_key_list:
            try:
                news = [str(list_key), str(news_dict[list_key])]
                if "（" in list_key or "(" in list_key:
                    n_title = re.sub(pattern, '', list_key)
                    news[0] = n_title
                text = ""
                resp = requests.get(news_dict[list_key])
                soup = BeautifulSoup(resp.text, "html.parser")
            except:
                raise NameError("errors?")

            for s in soup.find_all("p", class_="ynDetailText"):
                text += s.get_text()

            news.append(text)
            for r_count in range(1, 5):
                analysis_text = self.summarized(text, r_count)
                output_text = ''.join(analysis_text)
                news.append(str(output_text))

            news.append(datetime.datetime.now().strftime('%H%M%S'))
            news_list.append(news)
            tnews = tuple(news)
            news_tuple.append(tnews)
        return news_list, news_tuple

    """v2 current"""
    @staticmethod
    def get_jp_s_score(date):
        rec_list = [["team1", "team2", "date", "time", "score"]]
        rec_tuple = []

        i = 1
        strdate = date.strftime('%Y%m%d')

        while True:
            record = []
            # URL構築
            req = requests.get(r_base_url_soccer[0] +
                               strdate +
                               str(i).zfill(2))

            if req.status_code != 200:
                break

            try:
                soup = BeautifulSoup(req.text, "lxml")
            except:
                soup = BeautifulSoup(req.text, "html.parser")

            # チーム名取得
            try:
                div = soup.findAll("div", class_="name")

                for d in div:
                    record.append(d.string)
            except:
                continue

            record.append(strdate)
            record.append(datetime.datetime.now().strftime('%H%M%S'))

            # の成績
            try:
                td_home = soup.find("td", class_="home goal")
                td_away = soup.find("td", class_="away goal")
                record.append(td_home.string + "-" + td_away.string)
            except:
                continue
            rec_list.append(record)
            rec_tuple.append(tuple(record))

            i += 1

        return rec_list, rec_tuple

    """v2 current"""
    @staticmethod
    def get_jp_b_score(date):
        rec_list = [["team1", "team2", "date", "time", "score"]]
        rec_tuple = []

        i = 1
        strdate = date.strftime('%Y%m%d')

        while True:
            record = []
            names = []
            score = []
            # URL構築
            req = requests.get(r_base_url_baseball[0] +
                               strdate +
                               str(i).zfill(2) +
                               r_prop[0])

            if req.status_code != 200:
                break

            try:
                soup = BeautifulSoup(req.text, "lxml")
            except:
                soup = BeautifulSoup(req.text, "html.parser")

            # チーム名取得
            try:
                trs = soup.findAll("tr", class_="yjMS")

                for tr in trs:
                    b_tag = tr.find("b")
                    names.append(b_tag.string)
                    td = tr.find("td", class_="sum")
                    score.append(td.string)

            except:
                continue

            try:
                record.append(names[0])
                record.append(names[1])
                record.append(strdate)
                record.append(datetime.datetime.now().strftime('%H%M%S'))
                record.append(score[0] + "-" + score[1])

                rec_list.append(record)
                rec_tuple.append(tuple(record))
            except:
                pass
            i += 1

        return rec_list, rec_tuple

    @staticmethod
    def summarized(text, rowcount):
        try:
            sentences, debug_info = summarize(
                text, sent_limit=rowcount, continuous=True, debug=True
            )
        except:
            sentences = "sammarized error"

        return sentences


def main():
    RA = RecordAccumulation()

    for day in range(9, 20):
        today = datetime.date(2018, 5, day)

        test1, test1_t = RA.get_jp_bplayer_record(today)
        RA.save_csv(test1, "get_jp_bplayer_record_{}.csv".format(today.strftime("%m%d")))
        test2, test2_t = RA.get_jp_b_score(today)
        RA.save_csv(test2, "get_jp_b_score_{}.csv".format(today.strftime("%m%d")))
        test3, test3_t = RA.get_jp_s_score(today)
        RA.save_csv(test3, "get_jp_s_score_{}.csv".format(today.strftime("%m%d")))
        test4, test4_t = RA.news_check(today)
        RA.save_csv(test4, "news_check_{}.csv".format(today.strftime("%m%d")))


if __name__ == '__main__':
    main()

