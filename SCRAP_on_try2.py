#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep  6 14:22:17 2019

@author: corot
"""

#import des differentes librairies
import requests, json, sys, re, os, configparser
from bs4 import BeautifulSoup
#from sqlalchemy.sql import text
from sqlalchemy import create_engine, text
from urllib import parse
import pandas as pd
import argparse


#on selectionne le premier URL de base 
BASE = 'https://candidat.pole-emploi.fr'


#on configure la connexion a la BDD 
config = configparser.ConfigParser()
config.read_file(open(os.path.expanduser("datalab.cnf")))

DB = "Job1_Bot_FAG?charset=utf8"
TBL = "Job4_Bot_FAG"
CNF="myBDD"
engine = create_engine("mysql://%s:%s@%s/%s" % (config[CNF]['user'], parse.quote_plus(config[CNF]['password']), config[CNF]['host'], DB))

#on selectionne notre URL avec nos criteres de recherches ( region et données)
URL= BASE+'/offres/recherche?lieux=24R&motsCles=donn%C3%A9es&offresPartenaires=true&range=0-9&rayon=10&tri=0'
req = requests.get(URL)
soup = BeautifulSoup(req.text, "lxml")

#on fait sortir le nombre total d'offre sur notre recherche
str = re.findall(r'\d+', soup.select('h1.title')[0].text)[0] # REGEX pour le 1er chiffre !
N=int(str)
print("%d [%s]" % (N, URL))



# Boucle pour récup des offres, 100 par 100 avec export -> mySQL
statement = text("""
INSERT INTO Job4_Bot_FAG(ref, title, date1, dateLast, contrat, description, entreprise, dep, Ville, lien)
  VALUES(:ref, :title, :date, CURRENT_DATE(), :contrat, :description, :entreprise, :dep, :Ville, :lien)
ON DUPLICATE KEY
UPDATE
  title = :title,
  dateLast = CURRENT_DATE(),
  dep = :dep,
  lien= :lien,
  Ville = :Ville,
  entreprise = :entreprise
""")
#debut de la boucle
i=0
while(i<N):
    imax = i+99 if (i+99<N) else N
    if imax>N:
        imax=N
    rg="%d-%d" % (i, imax)
    print(rg)
    #URL avec rg en variable pour la range et la boucle 100 par 100
    URL=BASE+'/offres/recherche?lieux=24R&motsCles=donn%C3%A9es&offresPartenaires=true&range='+rg+'&rayon=10&tri=0'
    print("URL: "+URL)
    req = requests.get(URL)
    soup = BeautifulSoup(req.text, "lxml")
    list = soup.select('li.result')
    if len(list)==0:
        #return
        continue
    print("Nombre de blocks: %d [%s]" % (len(list), soup.select('h1.title')[0].text))
    for x in list:
        a = x.select('a.btn-reset')[0]
        title = a['title'][:80] # ATTENTION: VARCHAR(80) !
        href = a['href']
        id = href[25:]
        #print(x.select('a.btn-reset')[0]['title'])
        #print("%-80s %s" %(title, id))
        date1 = x.select('p.date')[0].text
        contrat = x.select('p.contrat')[0].text
        description = x.select('p.description')[0].text
        dep= (x.find('p', class_ ='subtext').find('span').text)[:3]
        entreprise = (x.find('p', class_ = 'subtext').text).split('-')[0]
        Ville= (x.find('p', class_ ='subtext').find('span').text)[5:]
        lien1 = BASE+x.find('a', class_ ='btn-reset')['href']
        param = {'ref': id, 'title':title, 'date':date1, 'contrat':contrat, 'description':description, 'entreprise':entreprise, 'dep':dep, 'Ville':Ville, 'lien':lien1}
        print(param)
        engine.execute(statement, param)
    
    #boucle par blocs de 100( juste au dessus on execute le sql)
    i+=100




#################################################################################################################
#    #dataframe technique classique 
#intitule = []
#date = []
#lien = []
#reference = []
#region=[]
#description=[]
#contrat=[]
#for result in soup.find_all(class_ ='result'):
#    intitule.append(result.find('h2', class_ ='t4 media-heading').text.replace('\n', ''))
#    date.append(result.find(class_ ="date").text.replace('\n', ''))
#    lien.append(result.find('a', class_ ='btn-reset')['href'])
#    reference.append(result['data-id-offre'])
#    #regex de la région
#    regi= str(result.find('p',class_='subtext'))
#    idx1=regi.index('<s')
#    idx2=regi.index('</s')
#    regi=regi[idx1:idx2]
#    region.append(regi.replace('<span>',''))
#    description.append(result.find('p', class_='description').text.replace('\n', ''))
#    contrat.append(result.find('p',class_='contrat').text.replace('\n', ''))
#
#data = {'Intitulé': intitule,
#        'Date parution': date,
#       'Lien': lien,
#       'Référence': reference,
#       'Region': region,
#       'Description' : description,
#       'Contrat' : contrat}
#df = pd.DataFrame(data)
#print(df)