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
SL = sportslive()

@app.route('/webhook', methods=['POST'])
def webhook():
    req = request.get_json(silent=True, force=True)
    res = processRequest(req)
    res = json.dumps(res, indent=4)
    r = make_response(res)
    r.headers['Content-Type'] = 'application/json'
    return r


def processRequest(req):
    actiontype = req.get("result").get("action")
    results = req.get("result")
    parameters = results.get("parameters")
    try:
        name = parameters.get("name")
    except:
        pass
    try:
        date = parameters.get("date")
    except:
        pass
    try:
        team1 = parameters.get("SoccerTeamName_for_Japan")
    except:
        pass
    try:
        team2 = parameters.get("SoccerTeamName_for_Japan1")
    except:
        pass
    try:
        team1 = parameters.get("BaseballTeamName_for_Japan")
    except:
        pass
    try:
        team2 = parameters.get("BaseballTeamName_for_Japan1")
    except:
        pass
    if date is None:
        date = datetime.datetime.now().strftime('%Y%m%d')
    else:
        date = date.strftime('%Y%m%d')

    if actiontype == "reply_to_player_record":
        res = SL.execute_sql(date, name, "bplayerrecord", "name", ["name", "record"])
    elif actiontype == "reply_to_news":
        res = SL.execute_sql(date, name, "newsrecord", "name", ["title", "row2_text"])
    elif actiontype == "reply_to_soccer_score" or actiontype == "reply_to_baseball_score":
        res = SL.execute_sql2(date, [team1, team2],"scorerecord", ["team1", "team2"], ["team1", "team2", "score"])

        if res == {}:
            res = SL.execute_sql2(date, [team2, team1], "scorerecord", ["team1", "team2"], ["team1", "team2", "score"])
    else:
        return {}

    return res


if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))

    print("Starting app on port %d" % port)

    app.run(debug=False, port=port, host='0.0.0.0')