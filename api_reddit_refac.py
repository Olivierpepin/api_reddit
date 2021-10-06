# ETL API Reddit : https://www.reddit.com/dev/api/

# L'objectif est de réaliser un ETL automatique qui récupère différentes données (images, textes, commentaires) des subreddits
# de Reddit : https://www.reddit.com/
# Par défaut le script est sur Dataisbeautiful (subreddit dédié à l'analyse de données) : https://www.reddit.com/r/dataisbeautiful/

# import dependencies

import requests
import pandas as pd
import time 
import urllib                   # Pour le requetage et le download.
import os                       # Fonction lié à l'operating system (Windows dans ce cas)
from dotenv import load_dotenv  # Pour le stockage de données sensibles (au root du projet) From https://pypi.org/project/python-dotenv/
                                # Pour plus d'information concernant dotENV : https://www.youtube.com/watch?v=YdgIWTYQ69A


# Permet d'obtenir le path du projet
BASEDIR = os.path.abspath(os.path.dirname(__file__))
# Attribution du path pour .env afin d'obtenir les variables d'environnements.
load_dotenv(os.path.join(BASEDIR, '.env'))


# Fonction de recuperation des données sensible dans .env et d'autentification a Reddit via une OAUth 2.0.
# DotENV doit toujours être au root du programme

def login():
    CLIENT_ID = os.getenv("CLIENT_ID")
    SECRET_ID = os.getenv("SECRET_ID")
    USERNAME_API =os.getenv("USERNAME_API")
    PASSWORD_API =os.getenv("PASSWORD_API")
    auth = requests.auth.HTTPBasicAuth(CLIENT_ID, SECRET_ID)
    data = {'grant_type': 'password','username': USERNAME_API,'password': PASSWORD_API}
    headers = {'User-Agent': 'api_test/0.0.1'}
    res = requests.post('https://www.reddit.com/api/v1/access_token', auth=auth, data=data, headers=headers)
    TOKEN = res.json()['access_token']
    headers['Authorization'] = f'bearer {TOKEN}'
    return headers


# Query d'un subreddit puis crawling dans le json avec récupération des champs "title" et "url". Intégration dans un DF.
# Attention cette méthode ne fonctionne que pour les .json avec seulement des dictionnaires.
# Voir la documentation de Reddit : https://www.reddit.com/dev/api/

def funct_query(headers):
    df = pd.DataFrame()
    reqe = requests.get('https://oauth.reddit.com/r/dataisbeautiful/new', headers=headers)
    reqe.json()
    for post in reqe.json()['data']['children']:
        df = df.append({
            'title': post['data']['title'],
            'url': post['data']['url']
        }, ignore_index=True)
    return df

# Recherche les différentes url dans le DF et garde seulement celles des variables locales. (A améliorer)

def transform(df):
    reddUrl = 'https://i.redd.it'
    imgUrl = 'https://i.imgur.com'
    liste_url = []
    for url in df['url']:
        if (reddUrl in url or imgUrl in url):liste_url.append(url)
    return liste_url

# Récupération du nom + extension du fichier puis fusion de ces deux élements dans un DataFrame (voir pour fusion dans dicotionnaire si possible).

def merging(liste_url):
    liste_file = [i.replace("https://i.redd.it/", "").replace("https://i.imgur.com/", "") for i in liste_url ]
    df_url = pd.DataFrame(liste_url)
    df_file = pd.DataFrame(liste_file)
    df_merged = pd.merge(df_url,df_file, left_index=True,right_index=True)
    return df_merged

# Appel d'une fonction qui download les images et les enregistrent automatiquement avec leurs noms d'origine.

def download_url(df_merged):
    i = 0
    start_time = time.time()
    while i < len(df_merged):
        test_url = df_merged['0_x'][i]
        name = df_merged['0_y'][i]
        urllib.request.urlretrieve(test_url, name)
        i = i+1
        if i == len(df_merged):break
        time.sleep(3)
    print("--- Completed in %s seconds ---" % (time.time() - start_time))


# Execution du code (Voir pour améliorations. Fonctions callback ? Class ?)

headers = login()

df = funct_query(headers)

liste_url = transform(df)

df_merged = merging(liste_url)

download_url(df_merged)






