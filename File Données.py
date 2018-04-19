# -*- coding: utf-8 -*-

import sys
"""reload(sys)
sys.setdefaultencoding('utf8')"""
# scipy
import scipy
# numpy
import numpy as np
from numpy.random import randint
# matplotlib
import matplotlib
# scikit-learn
import sklearn
# pymysql
import pymysql
# pandas
import pandas as pd

"""Connexion mysql"""
connection = pymysql.connect(user='root', password='root', 
                             database='padaone_ux_prod', host='localhost', port=3307)

"""Collecte des donnees"""
"""Sites surveilles"""
datraframesites = pd.read_sql("select idsite, name from piwik_site", connection)
datraframesites.to_csv("file01sites.csv", index=False, header=None)
print(datraframesites)

"""Visites des utilisateurs"""
dataframevisites = pd.read_sql("select idvisitor, visit_first_action_time,"
                               "idvisit, idsite from piwik_log_visit", connection)
dataframevisites = dataframevisites.sort_values(by=['idvisitor'], ascending=[True])
dataframevisites = dataframevisites.rename(columns={'visit_first_action_time': 'timestamp'})
dataframevisites = dataframevisites[['timestamp', 'idvisitor', 'idvisit', 'idsite']]
dataframevisites.to_csv("file02visites.csv", index=False, header=None)
print(dataframevisites)

"""Jointure visites et sites"""
dfvisitesjoin = pd.merge(datraframesites, dataframevisites, on=['idsite'])
dfvisitesjoin['type']="visit"
dfvisitesjoin = dfvisitesjoin[['timestamp', 'idvisitor', 'idvisit', 'idsite', 'name', 'type']]
dfvisitesjoin.to_csv("file03sitesvisites.csv", index=False, header=None)
print(dfvisitesjoin)

"""Actions des utilisateurs"""
datraframeactions = pd.read_sql("select idaction, name from piwik_log_action", connection)
datraframeactions.to_csv("file04actions.csv", index=False, header=None)
print(datraframeactions)

"""Liens visites et actions"""
dataframelink = pd.read_sql("select idvisitor, idsite, idvisit,"
                            "server_time, idaction_name from piwik_log_link_visit_action",
                            connection)
dataframelink = dataframelink.rename(columns={'idaction_name': 'idaction','server_time': 'timestamp'})
dataframelink = dataframelink[['timestamp', 'idvisitor', 'idvisit', 'idsite', 'idaction']]
dataframelink.to_csv("file05link.csv", index=False, header=None)
print(dataframelink)

"""Jointure entre actions et liens"""
dfactionjoin = pd.merge(dataframelink, datraframeactions, on=['idaction'], how='inner')
dfactionjoin['type']="action"
dfactionjoin = dfactionjoin.dropna()
dfactionjoin = dfactionjoin[['timestamp', 'idvisitor', 'idvisit', 'idsite', 'name', 'type']]
dfactionjoin.to_csv("file06linkactions.csv", index=False, header=None)
print(dfactionjoin)

"""Concatenations des actions et visites"""
df = dfvisitesjoin.append(dfactionjoin)

"""Conversion du string en integer"""
df = df.sort_values(by=['idvisitor'],ascending=['True'])

i=1
def trans_func():
    global i
    x=i
    i=i+1
    return x

df['idvisitor']=df.groupby('idvisitor').transform(lambda x:trans_func())
df = df.sort_values(by=['idvisitor'], ascending=[True])
print(df)

"""Production du fichier"""
df = df[['timestamp','idvisitor', 'idvisit', 'idsite', 'name', 'type']]
df = df.sort_values(by=['idvisit','timestamp', 'type'], ascending=[True, True, False])
df.to_csv("FileDonnees.csv", index=False, header=None)
print(df)