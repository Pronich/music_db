import sqlalchemy
import configparser
from pprint import pprint

def get_creds():
    config = configparser.ConfigParser()
    config.read("settings.ini")
    login = config['login']['login']
    password = config['password']['password']
    return [login, password]

def get_conn(creds, database):
    db = f'postgresql://{creds[0]}:{creds[1]}@localhost:5432/{database}'
    engine = sqlalchemy.create_engine(db)
    conn = engine.connect()
    return conn

if __name__ == '__main__':
    creds = get_creds()
    conn = get_conn(creds, 'music')

    print('-----------')
    # название и год выхода альбомов, вышедших в 2018 году;
    album_info = conn.execute('''
    select 
        name, release_year
    from album
    where 1=1
        and release_year = '2018';''').fetchall()
    pprint(album_info)

    print('\n-----------')

    # название и продолжительность самого длительного трека;
    tracks_name = conn.execute('''
    select
        name
    from tracks
    where 1=1
        and duration >= 210;''').fetchall()
    pprint(tracks_name)

    print('\n-----------')
    # названия сборников, вышедших в период с 2018 по 2020 год включительно;
    coll_info = conn.execute('''
    select
        name
    from collection
    where 1=1
        and release_year between '2018' and '2020';''').fetchall()
    pprint(coll_info)

    print('\n-----------')
    # исполнители, чье имя состоит из 1 слова;
    artist_name = conn.execute('''
    select
        name
    from artist
    where 1=1
     and name not like '%% %%';''').fetchall()
    pprint(artist_name)

    print('\n-----------')
    # название треков, которые содержат слово "мой"/"my".
    tracks_my = conn.execute('''
    select
        name
    from tracks
    where 1=1
        and name LIKE '%%мой%%'
        and name LIKE '%%my%%';''').fetchall()
    pprint(tracks_my)
