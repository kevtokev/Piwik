# -*- coding: utf-8 -*-
"""
Created on Fri Apr 13 11:26:42 2018

@author: C87478
"""
# pandas
import pandas as pd
from matplotlib import pyplot as plt
from graphviz import Digraph

"""Initialisation du dataframe"""
resu = pd.DataFrame()

"""Pour tous les sites"""
for k in range (2,3):
    """Lecture du fichier"""
    try:
        df = pd.read_csv("site"+str(k)+".csv",names=['timestamp', 'idvisitor', 'idvisit', 
                                          'idsite', 'entree', 'name'], header=None)
    except FileNotFoundError:
        continue
    
    """Tri selon idvisit, timestamp, type"""
    df = df.sort_values(by=['idvisit', 'timestamp'], ascending=[True, True])
    """On garde les colomnes interessantes"""
    df = df[['idvisit', 'name']]

    """Decalage des index"""
    prop1 = df
    prop2 = df.drop(0).reset_index()
    prop3 = df.drop([0, 1]).reset_index()
    prop4 = df.drop([0, 1, 2]).reset_index()
    prop5 = df.drop([0, 1, 2, 3]).reset_index()

    """Rajout de la colonne indice"""
    prop1['indice'] = prop1.index
    prop2['indice'] = prop2.index
    
    """Jointure d'une ligne et de sa suivante sur l'idvisit"""
    pattern2 = pd.merge(prop1, prop2, on=['indice','idvisit'], how='inner')
    pattern2 = pattern2.rename(columns={'name_x': 'prop1', 'name_y': 'prop2'})
    pattern2 = pattern2[['prop1', 'prop2']]
    pattern2 = pattern2.sort_values(by=['prop1','prop2'], ascending=[True,True])

    """Permet d'avoir le nombre total de prop1"""
    total = pattern2.groupby(['prop1']).size().reset_index(name='total')

    """Permet d'avoir la frequence du motif prop1 prop2"""
    prop = pattern2.groupby(['prop1','prop2']).size().reset_index(name='nb')

    """Jointure des deux dataframes"""
    res = pd.merge(prop, total, on=['prop1'], how='inner')

    """Division de la frequence par le total"""
    res['prob'] = res[['nb']].div(res['total'].values, axis=0)

    """Tri selon le motif"""
    res = res.sort_values(by=['prop1','prop2'], ascending=[True,True])

    """Mise en forme"""
    res = res[['prop1', 'prob', 'prop2']]

    """Ecriture du fichier"""
    res.to_csv("Prob"+str(k)+".csv", header=False, index=False)
    frames = [resu, res]
    resu = pd.concat(frames)

"""Ecriture du fichier contenant toutes les probabilités"""
resu.to_csv("Proba.csv", header=False, index=False)
print(resu)

resu=resu[:4]
G = Digraph(format='pdf')

G.attr(rankdir='LR', size='8,5')
G.attr('node', shape='circle')

nodelist = []
for idx, row in resu.iterrows():
    node1, prob, node2 = [str(i) for i in row]

    if node1 not in nodelist:
        G.node(node1)
        nodelist.append(node2)
    if node2 not in nodelist:
        G.node(node2)
        nodelist.append(node2)

    G.edge(node1,node2, label = prob)

G.render('sg', view=True)






