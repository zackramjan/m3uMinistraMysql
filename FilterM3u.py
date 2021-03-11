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
from datetime import datetime
import traceback

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
        program = findGuide(guide,i["tvg-ID"], tree)
    #sql.insertChannel(i["tvg-ID"],i["tvg-name"] + args.channel, genre, i["link"], i["tvg-logo"])
        for j in filterList:
            m = re.search(j,i["tvg-name"])
            m2 = re.search(j,program)
            if m or m2:
                if not program: program = i["tvg-name"]
                print "#EXTINF:-1 tvg-id=\"" + i["tvg-ID"] + "\" tvg-name=\"" + i["tvg-name"] + " - " + program + "\" tvg-logo=\"" + i["tvg-logo"] + "\" group-title=\"" + i["tvg-group"] + "\"," + i["tvg-name"] + " - " + program
                print i["link"]
                
def findGuide(guide, id, tree):
    if not id: return ""
    date = datetime.today().strftime('%Y%m%d%H%M%S')
    try: 
        t = tree.findall("programme[@channel='" +id + "']")
        for i in t: 
            #print(ET.dump(i));
            start = re.search("^\d+",i.get("start")).group(0)
            stop = re.search("^\d+",i.get("stop")).group(0)
            program =  i.find("title").text
            program = program.encode('ascii',errors='ignore')
            #print "      " + program + " " + start + " " + stop + " : " + date 
            if(int(date) > int(start) and int(date) < int(stop)): return program
    except: return ""
    return ""
    #except: traceback.print_exc(file=sys.stdout) 
    #m = re.search("(<programme start.*?channel=." + id + ".*?/programme>)",guide)
    #m = re.findall("<programme start=\"\d+ .\d+\" stop=\"\d+ .\d+\" channel=\"" + id + "\".*?\/programme>",guide)

if __name__ == '__main__':
    sys.exit(main())

