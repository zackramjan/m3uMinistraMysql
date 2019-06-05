'''
Created on May 27, 2019

@author: zack
'''

import mysql.connector


class MinistraSQL(object):
    '''
    classdocs
    '''

    def __init__(self, username, password, dbhost, prefixIn):
        '''
        Constructor
        '''
        self.prefix = prefixIn
        self.genreMap = {}
        self.myCon = mysql.connector.connect(
              host=dbhost,
              user=username,
              passwd=password,
              database="stalker_db"
            )
        self.TariffID = self.checkInsertTariff(prefixIn)

    def insertChannel(self, item):
        
        #check if channel already exits
        query = "select id from itv where name = %s AND cmd = %s"
        values = (item["tvg-name"],item["link"])
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        cursor.fetchone()   
        if (cursor.rowcount > 0):
            print "Skipping " + item["tvg-name"] + " : " + item["link"] + "  (already inserted)\n"
            return
        else:
            print "Inserting " + item["tvg-name"] + " : " + item["link"] + "\n"
            
        
        #insert/create the channels group as a genre and pkg
        #check the genre map file
        genre = item["tvg-group"]
        if  item["tvg-group"] in self.genreMap.keys():
            genre = self.genreMap[item["tvg-group"]]     
        self.checkInsertGenre(genre)
        gid = self.getGenreID(genre)
        pid = self.checkInsertPkg(genre)
        self.insertPkgIntoTariff(pid,self.TariffID)
        maxCh = self.getMaxChannel()
        
        #add teh channel
        query = "INSERT IGNORE INTO itv (name,number,cmd,base_ch,tv_genre_id,xmltv_id) VALUES( %s, %s, %s, 1, %s, %s)"
        values = (item["tvg-name"], maxCh, item["link"], gid, self.prefix + item["tvg-ID"])
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        chId = cursor.lastrowid
        self.myCon.commit()
        
        query = "INSERT  INTO ch_links (ch_id,url) VALUES( %s, %s)"
        values = (chId,item["link"])
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        self.myCon.commit()
        
        #insert the channel in the pkg
        query = "INSERT  INTO service_in_package (service_id,package_id,type,options) VALUES( %s, %s,%s,1)"
        values = (chId,pid,"tv")
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        self.myCon.commit()
        
    def checkInsertGenre(self,genre):
        maxGen = self.getMaxGenre()
        query = "INSERT IGNORE INTO tv_genre (title,number) VALUES (%s,%s)"
        values = (genre,maxGen)
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        self.myCon.commit()
        
    def checkInsertPkg(self,genre):
        pkgID = self.getPkgID(genre)
        if pkgID > 0:
            return pkgID
        
        query = "INSERT INTO services_package (external_id,name,type) VALUES (%s,%s,%s)"
        values = (genre,genre,"tv")
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        pkgID = cursor.lastrowid
        self.myCon.commit()    
        return pkgID
        
    def getGenreID(self,genre):
        query = "select id from tv_genre where title = %s"
        values = (genre,)
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        res = cursor.fetchone()   
        return res[0]
    
    def getPkgID(self,genre):
        query = "select id from services_package where name = %s"
        values = (genre,)
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        res = cursor.fetchone()   
        if (cursor.rowcount > 0):
            return res[0]
        else :
            return -1
    
    def getMaxChannel(self):
        query = "select max(number) as max from itv"
        cursor = self.myCon.cursor()
        cursor.execute(query)
        res = cursor.fetchone()   
        if (cursor.rowcount > 0 and res[0] is not None):
            return res[0] + 1
        else :
            return 1
    
    def getMaxGenre(self):
        query = "select max(number) as max from tv_genre"
        cursor = self.myCon.cursor()
        cursor.execute(query)
        res = cursor.fetchone()   
        if (cursor.rowcount > 0 and res[0] is not None):
            return res[0] + 1
        else :
            return 1
    
    def DeleteAllChannels(self):
        query = "delete from itv where id > 0"
        cursor = self.myCon.cursor()
        cursor.execute(query)
        self.myCon.commit()
        
        query = "delete from ch_links where id > 0"
        cursor = self.myCon.cursor()
        cursor.execute(query)
        self.myCon.commit()
    
    def DeleteAllGenre(self):
        query = "delete from tv_genre where id > 0"
        cursor = self.myCon.cursor()
        cursor.execute(query)
        self.myCon.commit()

    def checkInsertTariff(self,tariffName):
        if not tariffName:
            tariffName = "main"
        tariffID = self.getTariffID(tariffName)
        if tariffID > 0:
            return tariffID
        
        query = "INSERT INTO tariff_plan (name) VALUES (%s)"
        values = (tariffName,)
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        tariffID = cursor.lastrowid
        self.myCon.commit()    
        return tariffID
    
    def getTariffID(self,tariffName):
        query = "select id from tariff_plan where name = %s"
        values = (tariffName,)
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        res = cursor.fetchone()   
        if (cursor.rowcount > 0):
            return res[0]
        else :
            return -1
        
    def insertPkgIntoTariff(self,pkgID,TariffID):
        query = "select id from package_in_plan where package_id = %s AND plan_id = %s"
        values = (pkgID,TariffID)
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        res = cursor.fetchone()      
        if (cursor.rowcount < 1):
            query = "insert into package_in_plan (package_id,plan_id,optional) VALUES (%s,%s,1)"
            values = (pkgID,TariffID)
            cursor.execute(query, values)
            self.myCon.commit() 
    
    def useGenreMapFile(self,GenreMapFile):
        self.genreMap = {}
        with open(GenreMapFile) as f:
            for line in f:
                if ":" in line:
                    (key, val) = line.split(":")
                    self.genreMap[key] = val
        
            
