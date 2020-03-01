#!/usr/bin/python
'''
Created on May 27, 2019

@author: zack
'''

import sys
import M3uParser
import os
import argparse
import re
import xml.etree.cElementTree as ET

def main(argv=None): 
    parser = argparse.ArgumentParser(description='Process import args.')
    parser.add_argument('-x', '--xml', help='Input xmlGuide name')
    parser.add_argument_group('required arguments')
    requiredNamed = parser.add_argument_group('required arguments')
    requiredNamed.add_argument('-m', '--m3u', help='Input m3u name', required=True)
    requiredNamed.add_argument('-f', '--filter', help='genre mapping text file name in format: XMLTV-Group name:your genre  entries that end in ^ will cause that groups channels to be skipped',required=True)
    args=parser.parse_args()
    
    #read in the genreMap file
    filterList = [] 
    if os.path.isfile(args.filter):
        with open(args.filter) as f:
            for line in f:
                    filterList.append(line.rstrip())
    #read in xml                
    with open (args.xml, "r") as myfile:
        guide=myfile.read()
        tree = ET.parse(args.xml)

    #process the m3u and insert
    myM3u = M3uParser.M3uParser();
    myM3u.readM3u(args.m3u) 
    
    
    print "#EXTM3U"
    for i in myM3u.getList():
    #sql.insertChannel(i["tvg-ID"],i["tvg-name"] + args.channel, genre, i["link"], i["tvg-logo"])
        for j in filterList:
            m = re.search(j,i["tvg-name"])
            if m:
                print "#EXTINF:-1 tvg-id=\"" + i["tvg-ID"] + "\" tvg-name=\"" + i["tvg-name"] + "\" tvg-logo=\"" + i["tvg-logo"] + "\" group-title=\"" + i["tvg-group"] + "\"," + i["tvg-name"] 
                print i["link"]
                findGuide(guide,i["tvg-ID"], tree)
                
def findGuide(guide, id, tree):
    if not id: return
    try: 
        t = tree.findall("programme[@channel='" +id + "']")
        for i in t: 
            #print(ET.dump(i));
            print i.find("title").text
    except: print "hello"
    #m = re.search("(<programme start.*?channel=." + id + ".*?/programme>)",guide)
    #m = re.findall("<programme start=\"\d+ .\d+\" stop=\"\d+ .\d+\" channel=\"" + id + "\".*?\/programme>",guide)

if __name__ == '__main__':
    sys.exit(main())

