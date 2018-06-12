# -*- coding:utf8 -*-
# !/usr/bin/env python
# Copyright 2017 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function
from future.standard_library import install_aliases
install_aliases()

from urllib.parse import urlparse, urlencode
from urllib.request import urlopen, Request
from urllib.error import HTTPError
import sportslive
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account

import json
import os

from flask import Flask
from flask import request
from flask import make_response

# Flask app should start in global layout
app = Flask(__name__)
SL = sportslive.SportsLive()

@app.route('/webhook', methods=['POST'])
def webhook():
    req = request.get_json(silent=True, force=True)
    res = processRequest(req)
    res = json.dumps(res, indent=4)
    r = make_response(res)
    r.headers['Content-Type'] = 'application/json'
    return r


@app.route('/load-sql', methods=['GET'])
def load_sql():
    req = request.args.get('query')
    querylist = req.split('_')
    res = loadsqlRequest(querylist)
    res = json.dumps(res, indent=4)
    r = make_response(res)
    r.headers['Content-Type'] = 'application/json'
    return r


def loadsqlRequest(req):
    reqtype = None
    q1 = None
    q2 = None
    date = None
    
    try:
        reqtype = req[0]
    except:
        pass
    try:
        q1 = req[1]
    except:
        pass
    try:
        q2 = req[2]
    except:
        pass
    try:
        date = req[3]
    except:
        pass
    try:
        if date is None:
            date = q2
            if date is None:
                date = datetime.date.today().strftime('%Y%m%d') 
    except:
        pass
    try:
        if type(date) is list:            
            date = date[0].replace('-', '')
        else:
            date = date.replace('-', '')
    except:
        pass
    
    try:
        if reqtype == "p":
            res = SL.execute_sql(q1, "bplayerrecord", "name", ["name", "record"], day=date)
        elif reqtype == "n":
            res = SL.execute_sql(q1, "newsrecord", "title", ["title", "row2_text"], day=date)
        elif reqtype == "s":
            res = SL.execute_sql2([q1, q2],"scorerecord", ["team1", "team2"], ["team1", "team2", "score"], day=date)
        else:
            return {}
    except:
        return {}
        
    return res


def processRequest(req):
    actiontype = req.get("result").get("action")
    results = req.get("result")
    parameters = results.get("parameters")
    try:
        name = parameters.get("name")
        print(name)
    except:
        pass
    try:
        date = parameters.get("date")
        print(date)
    except:
        pass
    try:
        team1 = parameters.get("SoccerTeamName_for_Japan")
        print(team1)
    except:
        pass
    try:
        team2 = parameters.get("SoccerTeamName_for_Japan1")
        print(team2)
    except:
        pass
    try:
        if team1 is None:
            team1 = parameters.get("BaseballTeamName_for_Japan")
    except:
        pass
    try:
        if team2 is None:
            team2 = parameters.get("BaseballTeamName_for_Japan1")
    except:
        pass
    try:
        if type(date) is list:            
            date = date[0].replace('-', '')
        else:
            date = date.replace('-', '')
    except:
        pass
    print(actiontype)
    if actiontype == "reply_to_player_record":
        res = SL.execute_sql(name, "bplayerrecord", "name", ["name", "record"], day=date)
    elif actiontype == "reply_to_news":
        res = SL.execute_sql(name, "newsrecord", "title", ["title", "row2_text"], day=date)
    elif actiontype == "reply_to_soccer_score" or actiontype == "reply_to_baseball_score":
        res = SL.execute_sql2([team1, team2],"scorerecord", ["team1", "team2"], ["team1", "team2", "score"], day=date)
    else:
        return {}

    return res

@app.route('/news-loader', methods=['GET'])
def newsloader():
    json_dict = {}
    query = request.args.get('query')
    querylist = query.split('_')
    query = querylist[0]
    day = None
    rowcount = None
    if len(querylist) >= 2:
        rowcount = int(querylist[1])
    if len(querylist) >= 3:
        day = querylist[2]
    
    try:    
        if query is None:
            return 'No provided.', 400
        if rowcount is None:
            rowcount = 2
        if day is None:
            day = datetime.date.today()
            tdatetime = day.strftime('%Y-%m-%d')
        else:
            tdatetime = day
    except:
        json_dict.update({'error':
                         {
                             'text':query
                         }}
                         )
        encode_json_data = json.dumps(json_dict)
        return encode_json_data 
    
    try:
        result = SL.news_loader(query, rowcount, tdatetime)
        result = json.dumps(result, indent=4)
    except NameError as e:
        json_dict.update({'error':
                         {
                         'args':e.args,
                         'date':tdatetime    
                         }}
                         )
        encode_json_data = json.dumps(json_dict)
        return encode_json_data 
        
    except:
        json_dict.update({'error':
                         {
                         'date':"aaaaa"
                         }}
                         )
        encode_json_data = json.dumps(json_dict)
        return encode_json_data 
    
    if result is None:
        return 'not found : %s' % query, 400
    return result, 200


@app.route('/debug/news-loader', methods=['GET'])
def newsloader_debug():
    """Given an query, return that news debug mode."""
    query = request.args.get('query')
    querylist = query.split('_')
    query = querylist[0]
    day = None
    rowcount = None
    if len(querylist) >= 2:
        rowcount = int(querylist[1])
    if len(querylist) >= 3:
        day = querylist[2]
    json_dict = {}

    if query is None:
        return 'No provided.', 400
    if rowcount is None:
        rowcount = 2
    if day is None:
        day = datetime.date.today()
        tdatetime = day.strftime('%Y-%m-%d')
    else:
        tdatetime = day
    result = SL.news_loader(query, rowcount, tdatetime, debug=True)
    result = json.dumps(result, indent=4)
    if result is None:
        return 'not found : %s' % query, 400
    return result, 200


@app.route('/player-loader', methods=['GET'])
def playerloader():
    """Given an query, return that news."""
    query = request.args.get('query')
    querylist = query.split('_')
    query = querylist[0]
    day = None
    if len(querylist) > 2:
        day = querylist[1]
    json_dict = {}
    
    if query is None:
        return 'No provided.', 400
    if day is None:
        day = datetime.date.today()
        tdatetime = day.strftime('%Y-%m-%d')
    else:
        tdatetime = day
        
    result = SL.player_loader(query, tdatetime)
    result = json.dumps(result, indent=4)
    if result is None:
        return 'not found : %s' % query, 400
    return result, 200


@app.route('/debug/player-loader', methods=['GET'])
def playerloader_debug():
    """Given an query, return that news debug mode."""
    query = request.args.get('query')
    querylist = query.split('_')
    query = querylist[0]
    day = None
    if len(querylist) > 2:
        day = querylist[1]
    json_dict = {}

    if query is None:
        return 'No provided.', 400
    if day is None:
        day = datetime.date.today()
        day = day.strftime('%Y%m%d')
        tdatetime = day.strftime('%Y-%m-%d')
    else:
        tdatetime = day        
    result = SL.player_loader(query, tdatetime, debug=True)
    result = json.dumps(result, indent=4)
    if result is None:
        return 'not found : %s' % query, 400
    return result, 200


if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))

    print("Starting app on port %d" % port)

    app.run(debug=False, port=port, host='0.0.0.0')
