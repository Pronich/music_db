import sqlalchemy
from sqlalchemy import text
import configparser
from pprint import pprint


class Insert_data():
    def __init__(self, creds):
        self.creds = creds
        db = f'postgresql://{self.creds[0]}:{self.creds[1]}@localhost:5432/{self.creds[2]}'
        engine = sqlalchemy.create_engine(db)
        self.conn = engine.connect()

    def open_file(self, filename):
        with open(filename, 'r') as f:
            data = f.read().split('\n')
        return data

    def check_genre(self, track):
        check = text('''
                select id
                from genre
                where genre = :genre;''')
        insert = text('''
                insert into genre (genre) values (:genre);''')
        genre = track[6]
        cur = self.conn.execute(check, genre=genre).fetchall()
        if cur == []:
            self.conn.execute(insert, genre=genre)
            genre_id = self.conn.execute(check, genre=genre).fetchall()[0][0]
        else:
            genre_id = cur[0][0]
        return genre_id

    def check_collection(self, track):
        check = text('''
                    select id
                    from collection
                    where name = :coll;''')
        insert = text('''
                    insert into collection (name, release_year) values (:coll, :r_year);''')
        coll = track[4]
        r_year = track[5]
        cur = self.conn.execute(check, coll=coll).fetchall()
        if cur == []:
            self.conn.execute(insert, coll=coll, r_year=r_year)
            coll_id = self.conn.execute(check, coll=coll).fetchall()[0][0]
        else:
            coll_id = cur[0][0]
        return coll_id

    def check_artist(self, track):
        check = text('''
                        select id
                        from artist
                        where name = :artist;''')
        insert = text('''
                        insert into artist (name) values (:artist);''')
        artist = track[7]
        cur = self.conn.execute(check, artist=artist).fetchall()
        if cur == []:
            self.conn.execute(insert, artist=artist)
            artist_id = self.conn.execute(check, artist=artist).fetchall()[0][0]
        else:
            artist_id = cur[0][0]
        return artist_id

    def check_album(self, track):
        check = text('''
                        select id
                        from album
                        where name = :album;''')
        insert = text('''
                        insert into album (name, release_year) values (:album, :r_year);''')
        album = track[2]
        r_year = track[3]
        cur = self.conn.execute(check, album=album).fetchall()
        if cur == []:
            self.conn.execute(insert, album=album, r_year=r_year)
            album_id = self.conn.execute(check, album=album).fetchall()[0][0]
        else:
            album_id = cur[0][0]
        return album_id

    def check_art_genr(self, artist_id, genre_id):
        check = text('''
        select * 
        from artist_genre
        where artist_id = :artist_id and genre_id = :genre_id''')
        insert = text('''
                    insert into artist_genre (artist_id, genre_id) values (:artist_id, :genre_id)''')
        cur = self.conn.execute(check, artist_id=artist_id, genre_id=genre_id).fetchall()
        if cur == []:
            self.conn.execute(insert, artist_id=artist_id, genre_id=genre_id)
        return

    def check_art_alb(self, artist_id, album_id):
        check = text('''
        select * 
        from artist_album
        where artist_id = :artist_id and album_id = :album_id''')
        insert = text('''
                    insert into artist_album (artist_id, album_id) values (:artist_id, :album_id)''')
        cur = self.conn.execute(check, artist_id=artist_id, album_id=album_id).fetchall()
        if cur == []:
            self.conn.execute(insert, artist_id=artist_id, album_id=album_id)
        return

    def check_track_coll(self, track_id, coll_id):
        check = text('''
        select * 
        from track_collection
        where track_id=:track_id and collection_id=:coll_id;''')
        insert = text('''
                    insert into track_collection (track_id, collection_id) values (:track_id, :coll_id)''')
        cur = self.conn.execute(check, track_id=track_id, coll_id=coll_id).fetchall()
        if cur == []:
            self.conn.execute(insert, track_id=track_id, coll_id=coll_id)
        return

    def insert_to_db(self, filename):
        data = self.open_file(filename)
        for i in range(1, len(data)):
            row = data[i].split(';')
            genre_id = self.check_genre(row)
            coll_id = self.check_collection(row)
            artist_id = self.check_artist(row)
            album_id = self.check_album(row)
            track_name = row[0]
            duration = row[1]
            self.check_art_genr(artist_id, genre_id)
            self.check_art_alb(artist_id, album_id)
            insert = text('''
                    insert into track (album_id, name, duration)
                    values (:album_id, :name, :duration)''')
            self.conn.execute(insert, album_id=album_id, name=track_name, duration=duration)
            check = text('''
                    select id from track where name=:name''')
            track_id = self.conn.execute(check, name=track_name).fetchall()[0][0]
            self.check_track_coll(track_id, coll_id)
