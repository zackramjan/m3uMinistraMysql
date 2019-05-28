'''
Created on May 27, 2019

@author: zack
'''

import mysql.connector


class MinistraSQL(object):
    '''
    classdocs
    '''

    def __init__(self, username, password, dbhost):
        '''
        Constructor
        '''
        
        self.myCon = mysql.connector.connect(
              host=dbhost,
              user=username,
              passwd=password,
              database="stalker_db"
            )

    def insertChannel(self, item):
        self.checkInsertGenre(item["tvg-group"])
        gid = self.getGenreID(item["tvg-group"])
        maxCh = self.getMaxChannel()
        
        query = "INSERT  INTO itv (name,number,cmd,base_ch,tv_genre_id,xmltv_id) VALUES( %s, %s, %s, 1, %s, %s)"
        values = (item["tvg-name"], maxCh, item["link"], gid, item["tvg-ID"])
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        chId = cursor.lastrowid
        cursor.commit()
        
        query = "INSERT  INTO ch_links (ch_id,url) VALUES( %s, %s, %s, 1, %s, %s)"
        values = (item["link"], chId)
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        cursor.commit()

    def checkInsertGenre(self,genre):
        query = "INSERT IGNORE INTO tv_genre (title) VALUES( %s)"
        values = (genre)
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        cursor.commit()
        
    def getGenreID(self,genre):
        query = "select id from tv_genere where title = %s"
        values = (genre)
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        res = cursor.fetchone()   
        return res["title"]
    
    
    def getMaxChannel(self):
        query = "select max(number) as max from itv"
        cursor = self.myCon.cursor()
        cursor.execute(query)
        res = cursor.fetchone()   
        return res["max"]
    