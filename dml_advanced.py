import sqlalchemy
import configparser
from pprint import pprint

def get_creds():
    config = configparser.ConfigParser()
    config.read("settings.ini")
    login = config['login']['login']
    password = config['password']['password']
    database = config['DB']['db_name']
    return [login, password, database]

def get_conn(creds):
    db = f'postgresql://{creds[0]}:{creds[1]}@localhost:5432/{creds[2]}'
    engine = sqlalchemy.create_engine(db)
    conn = engine.connect()
    return conn

if __name__ == '__main__':
    creds = get_creds()
    conn = get_conn(creds)

    print('-----------')
    # количество исполнителей в каждом жанре;
    artist_cnt = conn.execute('''
    select 
        ge.genre,
        count(ag.artist_id)
    from artist_genre ag
    inner join genre ge
        on ag.genre_id=ge.id
    group by 1;''').fetchall()
    pprint(artist_cnt)

    print('\n-----------')

    # количество треков, вошедших в альбомы 2019-2020 годов;
    tracks_cnt = conn.execute('''
    select
        count(tr.name)
    from track tr
    inner join album al
        on tr.album_id=al.id
        and release_year between '2019' and '2020';''').fetchall()
    pprint(tracks_cnt)

    print('\n-----------')

    # средняя продолжительность треков по каждому альбому;
    avg_dur = conn.execute('''
    select
        al.name,
        avg(tr.duration)
    from track tr
    inner join album al
        on tr.album_id = al.id
    group by 1;''').fetchall()
    pprint(avg_dur)

    print('\n-----------')
    # все исполнители, которые не выпустили альбомы в 2020 году;
    artists_not_2020 = conn.execute('''
    select distinct
        ar.name
    from artist_album aa
    inner join artist ar
        on aa.artist_id = ar.id
    inner join album al
        on aa.album_id = al.id
        and al.release_year != '2020';''').fetchall()
    pprint(artists_not_2020)

    print('\n-----------')
    # названия сборников, в которых присутствует конкретный исполнитель (выберите сами); надо испраивть
    artist_in_coll = conn.execute('''
    select
        col.name
    from artist ar
    inner join artist_album aa
        on ar.id = aa.artist_id
    inner join album al
        on aa.album_id = al.id
    inner join track tr
        on al.id = tr.album_id
    inner join track_collection tc
        on tr.id = tc.track_id
    inner join collection col
        on tc.collection_id = col.id
    where ar.name = 'Лера Яскевич'
    ;''').fetchall()
    pprint(artist_in_coll)

    print('\n-----------')
    # название альбомов, в которых присутствуют исполнители более 1 жанра;
    album_name = conn.execute('''
        select
            al.name
        from album al
        inner join artist_album aa
            on aa.album_id = al.id
        where 1=1
            and artist_id in (select artist_id
                from artist_genre
                group by artist_id
                having count(genre_id)>1)
        ;''').fetchall()
    pprint(album_name)

    print('\n-----------')
    # наименование треков, которые не входят в сборники;
    track_not_coll = conn.execute('''
        select
            tr.name
        from track tr
        inner join track_collection tc
            on tc.track_id = tr.id
        inner join collection col
            on tc.collection_id=col.id
            and col.name = '';
        ''').fetchall()
    pprint(track_not_coll)

    print('\n-----------')
    # исполнителя(-ей), написавшего самый короткий по продолжительности трек (теоретически таких треков может быть несколько);
    short_track_artist = conn.execute('''
        select
            ar.name
        from artist ar
        inner join artist_album aa
            on aa.artist_id = ar.id
        inner join track tr
            on tr.album_id = aa.album_id
            and tr.duration = (select duration
            from track
            order by duration
            limit 1)
        ;''').fetchall()
    pprint(short_track_artist)

    print('\n-----------')
    # название альбомов, содержащих наименьшее количество треков.
    album_min_track = conn.execute('''
        select distinct
            al.name
        from album al
        inner join track tr
            on tr.album_id=al.id
        where 1=1
            and tr.album_id in (select album_id
             from track
             group by album_id
             having count(id) = (select count(id) as cnt from track group by album_id order by cnt limit 1))
             ;''').fetchall()
    pprint(album_min_track)