# m3uMinistraMysql
## insert m3u playlist directly to ministra database

* edit ImportM3u with your ministra mysql info


then run:

``` 
usage: ImportM3u.py [-h] [-t TAG] [-g GENRE] [-n CHANNEL] -m M3U

Process import args.

optional arguments:
  -h, --help            show this help message and exit
  -t TAG, --tag TAG     xmltv-id prefix: any tag/id of your choice. channels
                        will be added to a tariff with this name
  -g GENRE, --genre GENRE
                        genre mapping text file name in format: XMLTV-Group
                        name:your genre
  -n CHANNEL, --channel CHANNEL
                        A prefix for your channel names

required arguments:
  -m M3U, --m3u M3U     Input m3u name

 ```
 