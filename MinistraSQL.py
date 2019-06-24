'''
Created on May 27, 2019

@author: zack
'''

import mysql.connector
import pickle
import subprocess
import os
import re
import time

class MinistraSQL(object):
    '''
    classdocs
    '''

    def __init__(self, username, password, dbhost, prefixIn):
        '''
        Constructor
        '''
        self.curDateTime = time.strftime('%Y-%m-%d %H:%M:%S')
        self.prefix = prefixIn
        self.chanCache = {}
        self.myCon = mysql.connector.connect(
              host=dbhost,
              user=username,
              passwd=password,
              database="stalker_db"
            )
        self.maxChannelCached = self.getMaxChannel() + 1
        self.screenshotsDir="/var/www/html/stalker_portal/screenshots"
        self.logoDir="/var/www/html/stalker_portal/misc/logos"
        self.loadCache()
        
    def insertChannel(self, itemID,itemName, itemGroup, itemLink, itemPic):
        if not itemGroup:
            itemGroup="Misc"
        #check if channel already exits
        query = "select id from itv where name = %s AND cmd = %s"
        values = (itemName,itemLink)
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        cursor.fetchone()   

        #we already have this channel so update the timestamp.
        if (cursor.rowcount > 0):
            print "   Skipping Channel [" +itemGroup + "] " +itemName + " : " + itemLink + "  (already inserted)"
            query = "update itv set modified = %s where name = %s AND cmd = %s"
            values = (self.curDateTime,itemName,itemLink)
            cursor.execute(query, values)
            cursor = self.myCon.cursor()
            cursor.execute(query, values)
            self.myCon.commit()
            return
        else:
            print "Inserting Channel [" +itemGroup + "] " +itemName + " : " + itemLink 
        
        #insert/create the channels group as a genre and pkg
        tid = self.checkInsertTariff(self.prefix)
        self.checkInsertGenre(itemGroup)
        gid = self.getGenreID(itemGroup)
        pid = self.checkInsertPkg(self.prefix + "-" + itemGroup,False,"tv")
        self.insertPkgIntoTariff(pid,tid,True)
        maxCh = self.getMaxChannel()
        if (maxCh < self.maxChannelCached):
            maxCh = self.maxChannelCached
        
        #check if we have previously set a channel number in previous run cache.
        if("tv" + itemGroup + itemName in self.chanCache):
            #print "\tfound cached channel"
            maxCh = self.chanCache["tv" + itemGroup + itemName]
        
        self.chanCache["tv" + itemGroup + itemName] = maxCh;
        
        #add channel logo
        logoFile = re.sub('[^0-9a-zA-Z]+', '_', itemGroup + itemName) + ".jpg"
        logoPath = self.logoDir + "/original/" + logoFile
        
        if(itemPic and not os.path.isfile(logoPath)):
            subprocess.call("curl -m 10 -o \"" + logoPath + "\"" " \"" + itemPic + "\"" , shell=True)
            #convert "input.jpg" -resize 500x500! -quality 100 "output.png""
            subprocess.call("convert \"" + logoPath + "\"" + " -resize 36x36! -quality 100  \"" + self.logoDir + "/120/" + logoFile + "\"" , shell=True)
            subprocess.call("convert \"" + logoPath + "\"" + " -resize 48x48! -quality 100  \"" + self.logoDir + "/160/" + logoFile + "\"" , shell=True)
            subprocess.call("convert \"" + logoPath + "\"" + " -resize 72x72! -quality 100  \"" + self.logoDir + "/240/" + logoFile + "\"" , shell=True)
            subprocess.call("convert \"" + logoPath + "\"" + " -resize 96x96! -quality 100  \"" + self.logoDir + "/320/" + logoFile + "\"" , shell=True)
            
             
        if not os.path.isfile(self.logoDir + "/120/" + logoFile):
            logoFile = ""      
        
        xmlID = self.prefix +  itemID
        if not itemID:
            xmlID = ""         
        
        #add the channel
        query = "INSERT IGNORE INTO itv (name,number,cmd,base_ch,tv_genre_id,xmltv_id,logo,modified) VALUES( %s, %s, %s, 1, %s, %s, %s, %s)"
        values = (itemName, maxCh, itemLink, gid, xmlID,logoFile,self.curDateTime)
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
        
    def checkInsertPkg(self,genre,isAllServices,mediaType):
        pkgID = self.getPkgID(genre)
        if pkgID > 0:
            return pkgID
        
        query = "INSERT INTO services_package (external_id,name,type,all_services) VALUES (%s,%s,%s,%s)"
        all_services = 1 if isAllServices else 0
        values = (genre,genre,mediaType,all_services)
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
        
    def insertPkgIntoTariff(self,pkgID,TariffID,isOptional):
        query = "select id from package_in_plan where package_id = %s AND plan_id = %s"
        values = (pkgID,TariffID)
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        cursor.fetchone()      
        if (cursor.rowcount < 1):
            query = "insert into package_in_plan (package_id,plan_id,optional) VALUES (%s,%s,%s)"
            optional = 1 if isOptional else 0
            values = (pkgID,TariffID,optional)
            cursor.execute(query, values)
            self.myCon.commit() 
        
    def insertMovie(self, itemID,itemName, itemGroup, itemLink, itemPic):
        if not itemGroup:
            itemGroup="Misc"
        #check if channel already exits
        query = "select id from video where name = %s AND path = %s"
        values = (itemName,itemLink)
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        cursor.fetchone()   
        if (cursor.rowcount > 0):
            print "   Skipping Movie [" +itemGroup + "] " + itemName + " : " +itemLink + "  (already inserted)"
            query = "update video set added = %s where name = %s AND path = %s"
            values = (self.curDateTime,itemName,itemLink)
            cursor.execute(query, values)
            cursor = self.myCon.cursor()
            cursor.execute(query, values)
            self.myCon.commit()
            return
        else:
            print "Inserting Movie [" +itemGroup + "] " + itemName + " : " +itemLink 
        
        self.checkInsertVideoCat(itemGroup)
        gid = self.getVideoCatID(itemGroup)
        
        #add the movie
        query = "INSERT IGNORE INTO video (name,o_name,path,category_id,status,autocomplete_provider,protocol,accessed,added) VALUES( %s, %s, %s, %s, %s, %s,%s,%s,%s)"
        values = (itemName,itemName,itemLink, gid,1,"tmdb","",1,self.curDateTime)
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        vidId = cursor.lastrowid
        self.myCon.commit()
                
        #add movie link
        query = "INSERT IGNORE INTO video_series_files (video_id,file_type,protocol,url,languages,status,accessed) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        values = (vidId,"video","custom","ffmpeg" + " " + itemLink, "a:1:{i:0;s:2:\"en\";}",1,1)
        cursor = self.myCon.cursor()
        cursor.execute(query, values)
        self.myCon.commit()
        
        #add movie screenshot
        screenshot = self.screenshotsDir + "/" + re.sub('[^0-9a-zA-Z]+', '_', itemGroup + itemName)
        if(itemPic and not os.path.isfile(screenshot)):
            subprocess.call("curl -m 10 -o \"" + screenshot + "\"" " \"" + itemPic + "\"" , shell=True)
             
        if(os.path.isfile(screenshot)):
            query = "INSERT IGNORE INTO screenshots (id,type,media_id) VALUES( %s, %s, %s)"
            values = (vidId,"image/jpeg",vidId)
            cursor = self.myCon.cursor()
            cursor.execute(query, values)
            self.myCon.commit()
            imageDir = -((-1 * vidId) // 100)
            if not os.path.isdir(self.screenshotsDir + "/" + str(imageDir)):
                os.mkdir(self.screenshotsDir + "/" + str(imageDir))
            subprocess.call("ln -s \"" + screenshot + "\"" " \"" + self.screenshotsDir + "/" + str(imageDir) + "/" + str(vidId) + ".jpg\"" , shell=True)                
        
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
    
    def cleanPackagesAndTariffs(self):
        print "deleting all packages and tariffs"
        self.executeStatement("delete from tariff_plan")
        self.executeStatement("delete from package_in_plan")
        self.executeStatement("delete from user_package_subscription")
        self.executeStatement("delete from services_package")
    
    def cleanChannels(self):
        print "deleting all channels"
        self.executeStatement("delete from itv")
        self.executeStatement("delete from ch_links")
        self.executeStatement("delete from tv_genre")
        self.executeStatement("delete from service_in_package")
        self.executeStatement("delete from ch_links")
        self.executeStatement("delete from played_itv")
        
    def cleanMovies(self):
        print "deleting all movies"
        self.executeStatement("delete from video")
        self.executeStatement("delete from video_series_files")
        self.executeStatement("delete from media_category")
        self.executeStatement("delete from cat_genre")
        self.executeStatement("delete from screenshots")
        
    def insertAllChannelsAndMoviesForAllUsers(self):
        print "adding allmovies and allchannels as main tariff for all users"
        pidtv = self.checkInsertPkg("allchannels",True,"tv")
        pidmov = self.checkInsertPkg("allmovies",True,"video")
        tid= self.checkInsertTariff("everything")
        self.insertPkgIntoTariff(pidtv,tid,False)
        self.insertPkgIntoTariff(pidmov,tid,False)
        self.executeStatement("UPDATE users set tariff_plan_id = " + str(tid))
        
    def executeStatement(self,sql):
        cursor = self.myCon.cursor()
        cursor.execute(sql)
        self.myCon.commit()  
    
    def loadCache(self):
        if(os.path.isfile("MinistraSQL.cache")):
            with open('MinistraSQL.cache', 'rb') as handle:
                self.chanCache = pickle.load(handle)
       
    def saveCache(self):
        with open('MinistraSQL.cache', 'wb') as handle:
            pickle.dump(self.chanCache, handle, protocol=pickle.HIGHEST_PROTOCOL)

                
