'''
Created on May 27, 2019

@author: zack
'''

import sys
import M3uParser
import MinistraSQL

def main(argv=None): 
    
    
    
    
    
    myM3u = M3uParser.M3uParser();
    myM3u.readM3u(sys.argv[1])
    sql = MinistraSQL.MinistraSQL("username","password","localhost",argv[2]) 
    for i in myM3u.getList():
        sys.stderr.write(i["title"] + " ^ " + i["tvg-name"] + " ^ " + i["tvg-ID"] + " ^ " + i["tvg-group"] + " ^ " + i["link"] + "\n")
        sql.insertChannel(i)

if __name__ == '__main__':
    sys.exit(main())

