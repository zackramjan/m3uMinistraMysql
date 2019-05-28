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
        self.myCon.commit()
        
        query = "INSERT  INTO ch_links (ch_id,url) VALUES( %s, %s)"
        values = (chId,item["link"])
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        self.myCon.commit()

    def checkInsertGenre(self,genre):
        query = "INSERT IGNORE INTO tv_genre (title) VALUES (%s)"
        values = (genre,)
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        self.myCon.commit()
        
    def getGenreID(self,genre):
        query = "select id from tv_genre where title = %s"
        values = (genre,)
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        res = cursor.fetchone()   
        return res[0]
    
    
    def getMaxChannel(self):
        query = "select max(number) as max from itv"
        cursor = self.myCon.cursor()
        cursor.execute(query)
        res = cursor.fetchone()   
        return res[0] + 1;
    
