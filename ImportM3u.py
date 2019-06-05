#!/usr/bin/python
'''
Created on May 27, 2019

@author: zack
'''

import sys
import M3uParser
import MinistraSQL
import os

def main(argv=None): 
    myM3u = M3uParser.M3uParser();
    myM3u.readM3u(sys.argv[1])
    sql = MinistraSQL.MinistraSQL("root","st@lk3r","localhost",sys.argv[2])
    
    if(os.path.isfile(sys.argv[3])):
        sql.useGenreMapFile(sys.argv[3])
     
    for i in myM3u.getList():
        if "/series/" not in i["link"] and "/movie/" not in i["link"]:
            sys.stderr.write(i["title"] + " ^ " + i["tvg-name"] + " ^ " + i["tvg-ID"] + " ^ " + i["tvg-group"] + " ^ " + i["link"] + "\n")
            sql.insertChannel(i)

if __name__ == '__main__':
    sys.exit(main())

