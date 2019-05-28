'''
Created on May 27, 2019

@author: zack
'''

import sys
import os
import mysql.connector

import M3uParser

def main(argv=None): 
    myM3u = M3uParser();
    myM3u.readM3u(sys.argv[1])
    for i in myM3u.getList():
        s = i["title"] + " ^ " + i["tvg-name"] + " ^ " + i["tvg-ID"] + " ^ " + i["tvg-group"] + " ^ " + i["link"]
        sys.stderr.write(s + "\n")
    
    
    
    
    


if __name__ == '__main__':
    sys.exit(main())
