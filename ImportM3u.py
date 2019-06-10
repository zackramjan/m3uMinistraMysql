#!/usr/bin/python
'''
Created on May 27, 2019

@author: zack
'''

import sys
import M3uParser
import MinistraSQL
import os
import argparse

def main(argv=None): 
    parser = argparse.ArgumentParser(description='Process import args.')
    parser.add_argument('-t', '--tag', help='xmltv-id prefix: any tag/id of your choice. channels will be added to a tariff with this name',default="")
    parser.add_argument('-g', '--genre', help='genre mapping text file name in format: XMLTV-Group name:your genre')
    parser.add_argument('-n', '--channel', help='A prefix for your channel names', default="")
    parser.add_argument('-r', '--remove', help='clear out existing channels first',action='store_true')
    requiredNamed = parser.add_argument_group('required arguments')
    requiredNamed.add_argument('-m', '--m3u', help='Input m3u name', required=True)
    args=parser.parse_args()
    
    sql = MinistraSQL.MinistraSQL("root","st@lk3r","localhost",args.tag)
    
    if args.remove:
        sql.cleanChannels()
        
    myM3u = M3uParser.M3uParser();
    myM3u.readM3u(args.m3u)
    
    if os.path.isfile(args.genre):
        sql.useGenreMapFile(args.genre)
     
    for i in myM3u.getList():
        if "/series/" not in i["link"] and "/movie/" not in i["link"]:
            sql.insertChannel(i["tvg-ID"] + args.channel,i["tvg-name"], i["tvg-group"],  i["link"], i["tvg-logo"])

if __name__ == '__main__':
    sys.exit(main())

