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
        self.myCon = mysql.connector.connect(
              host=dbhost,
              user=username,
              passwd=password,
              database="stalker_db"
            )
        self.TariffID = self.checkInsertTariff(prefixIn)

    def insertChannel(self, itemID,itemName, itemGroup, itemLink, itemPic):
        #check if channel already exits
        query = "select id from itv where name = %s AND cmd = %s"
        values = (itemName,itemLink)
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        cursor.fetchone()   
        if (cursor.rowcount > 0):
            print "   Skipping Channel " +itemName + " : " + itemLink + "  (already inserted)"
            return
        else:
            print "Inserting Channel " +itemName + " : " + itemLink 
        
        #insert/create the channels group as a genre and pkg
        self.checkInsertGenre(itemGroup)
        gid = self.getGenreID(itemGroup)
        pid = self.checkInsertPkg(self.prefix + "-" + itemGroup)
        self.insertPkgIntoTariff(pid,self.TariffID)
        maxCh = self.getMaxChannel()
        
        #add the channel
        query = "INSERT IGNORE INTO itv (name,number,cmd,base_ch,tv_genre_id,xmltv_id) VALUES( %s, %s, %s, 1, %s, %s)"
        xmlID = self.prefix +  itemID
        if not itemID:
            xmlID = ""     
        values = (itemName, maxCh, itemLink, gid, xmlID)
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        chId = cursor.lastrowid
        self.myCon.commit()
        
        query = "INSERT  INTO ch_links (ch_id,url) VALUES( %s, %s)"
        values = (chId,itemLink)
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
        
    def cleanChannels(self):
        query = "delete from itv"
        cursor = self.myCon.cursor()
        cursor.execute(query)
        query = "delete from ch_links"
        cursor = self.myCon.cursor()
        cursor.execute(query)
        query = "delete from service_in_package"
        cursor = self.myCon.cursor()
        cursor.execute(query)
        self.myCon.commit() 
    
    def insertMovie(self, itemID,itemName, itemGroup, itemLink, itemPic):
        
        #check if channel already exits
        query = "select id from video where name = %s AND path = %s"
        values = (itemName,itemLink)
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        cursor.fetchone()   
        if (cursor.rowcount > 0):
            print "   Skipping Movie " + itemName + " : " +itemLink + "  (already inserted)"
            return
        else:
            print "Inserting Movie " + itemName + " : " +itemLink 
        
        self.checkInsertVideoCat(itemGroup)
        gid = self.getVideoCatID(itemGroup)
        
        #add the movie
        query = "INSERT IGNORE INTO video (name,o_name,path,category_id,status,autocomplete_provider) VALUES( %s, %s, %s, %s, %s, %s)"
        values = (itemName,itemName,itemLink, gid,1,"tmdb")
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        vidId = cursor.lastrowid
        self.myCon.commit()
                
        #add movie link
        query = "INSERT IGNORE INTO video_series_files (video_id,file_type,protocol,url,languages,status) VALUES (%s,%s,%s,%s,%s,%s)"
        values = (vidId,"video","custom","ffmpeg" + " " + itemLink, "a:1:{i:0;s:2:\"en\";}",1)
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        self.myCon.commit()
        
    def checkInsertVideoCat(self,genre):
        maxGen = self.getMaxMovieCat()
        query = "INSERT IGNORE INTO media_category (category_name,category_alias,num) VALUES (%s,%s,%s)"
        values = (genre,genre,maxGen)
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        self.myCon.commit()
           
    def getVideoCatID(self,genre):
        query = "select id from media_category where category_name = %s"
        values = (genre,)
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        res = cursor.fetchone()   
        return res[0]
    
    def getMaxMovieCat(self):
        query = "select max(num) as max from media_category"
        cursor = self.myCon.cursor()
        cursor.execute(query)
        res = cursor.fetchone()   
        if (cursor.rowcount > 0 and res[0] is not None):
            return res[0] + 1
        else :
            return 1    

    def cleanMovies(self):
        query = "delete from video"
        cursor = self.myCon.cursor()
        cursor.execute(query)
        query = "delete from video_series_files"
        cursor = self.myCon.cursor()
        cursor.execute(query)
        query = "delete from media_category"
        cursor = self.myCon.cursor()
        cursor.execute(query)
        query = "delete from cat_genre"
        cursor = self.myCon.cursor()
        cursor.execute(query)
        self.myCon.commit()     
