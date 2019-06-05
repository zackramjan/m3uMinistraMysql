#!/usr/bin/python3
# Purpose:    
# Usage:    
# Author:    Timmy93
# Date:        
# Version:    
# Disclaimer:    

import os
import re
import urllib2
import random

class M3uParser:
    
    def __init__(self):
        self.files = []
      
    
    #Download the file from the given url
    def downloadM3u(self, url, filename):
        currentDir = os.path.dirname(os.path.realpath(__file__))
        if not filename:
            filename = "test.m3u"
        try:
            filename = os.path.join(currentDir, filename)
            urllib2.Request.urlretrieve(url, filename)
        except:
            print("Cannot download anything from the url\nHave you modified the ini file?")
            exit()
        self.readM3u(filename)
    
    #Read the file from the given path
    def readM3u(self, filename):
        self.filename = filename
        self.readAllLines()
        self.parseFile()

    #Read all file lines
    def readAllLines(self):
        self.lines = [line.rstrip('\n') for line in open(self.filename)]
        return len(self.lines)
    
    def parseFile(self):
        numLine = len(self.lines)
        for n in range(numLine):
            line = self.lines[n]
            if line[0] == "#":
                #print("Letto carattere interessante")
                self.manageLine(n)
    
    def manageLine(self, n):
        lineInfo = self.lines[n]
        lineLink = self.lines[n+1]
        if lineInfo != "#EXTM3U":
            m = re.search("tvg-name=\"(.*?)\"", lineInfo, re.IGNORECASE)
            name = m.group(1)
            m = re.search("tvg-ID=\"(.*?)\"", lineInfo, re.IGNORECASE)
            id = m.group(1)
            m = re.search("tvg-logo=\"(.*?)\"", lineInfo, re.IGNORECASE)
            logo = m.group(1)
            m = re.search("group-title=\"(.*?)\"", lineInfo, re.IGNORECASE)
            group = m.group(1)
            m = re.search("[,](?!.*[,])(.*?)$", lineInfo, re.IGNORECASE)
            title = m.group(1)
            # ~ print(name+"||"+id+"||"+logo+"||"+group+"||"+title)
            
            test = {
                "title": title.rstrip(),
                "tvg-name": name.rstrip(),
                "tvg-ID": id.rstrip(),
                "tvg-logo": logo.rstrip(),
                "tvg-group": group.rstrip(),
                "titleFile": os.path.basename(lineLink).rstrip(),
                "link": lineLink.rstrip()
            }
            self.files.append(test)
            
    def exportJson(self):
        #TODO
        print("Not implemented")
    
    #Remove files with a certain file extension
    def filterOutFilesEndingWith(self, extension):
        self.files = list(filter(lambda file: not file["titleFile"].endswith(extension), self.files))
    
    #Select only files with a certain file extension
    def filterInFilesEndingWith(self, extension):
        #Use the extension as list
        if not isinstance(extension, list):
            extension = [extension]
        if not len(extension):
            
            return
        new = []
        #Iterate over all files and extensions
        for file in self.files:    
            for ext in extension:
                if file["titleFile"].endswith(ext):
                    #Allowed extension - go to next file
                    new.append(file)
                    break
      
        self.files = new
    
    #Remove files that contains a certain filterWord
    def filterOutFilesOfGroupsContaining(self, filterWord):
        self.files = list(filter(lambda file: filterWord not in file["tvg-group"], self.files))

    #Select only files that contais a certain filterWord
    def filterInFilesOfGroupsContaining(self, filterWord):
        #Use the filter words as list
        if not isinstance(filterWord, list):
            filterWord = [filterWord]
        if not len(filterWord):

            return
        new = []
        for file in self.files:
            for fw in filterWord:    
                if fw in file["tvg-group"]:
                    #Allowed extension - go to next file
                    new.append(file)
                    break
        self.files = new

    #Getter for the list
    def getList(self):
        return self.files
        
    #Return the info assciated to a certain file name
    def getCustomTitle(self, originalName):
        result = list(filter(lambda file: file["titleFile"] == originalName, self.files))
        if len(result):
            return result
        else:
            print("No file corresponding to: "+originalName)

    #Return a random element
    def getFile(self, randomShuffle):
        if randomShuffle:
            random.shuffle(self.files)
        if not len(self.files):

            return None
        return self.files.pop()
