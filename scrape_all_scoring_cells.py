from s2sphere import *
import requests
import random
import time
import sqlite3
import json

class ScrapeAllCellInfo:
    def __init__(self, dbfile):
        self.con = sqlite3.connect(dbfile)
        region1=LatLngRect(LatLng.from_degrees(-90,0),LatLng.from_degrees(90,180))
        r1=RegionCoverer()
        r1.min_level,r1.max_level=(6,6)
        cell_IDs1 = r1.get_covering(region1)
        region2=LatLngRect(LatLng.from_degrees(-90,180),LatLng.from_degrees(90,0))
        r2=RegionCoverer()
        r2.min_level,r2.max_level=(6,6)
        cell_IDs2 = r2.get_covering(region2)
        
        self.fd = open("all.json", "w")
        self.fd.write("{ entrys : [\n")
        self.all_cell_IDs = set(cell_IDs1) | set(cell_IDs2)
        self.cout = 0
        random.seed()
        print(len(self.all_cell_IDs))

        self.ffail = open("fails", "w")
        self.ffailCount = 0

        self.cur = self.con.cursor()
        if self.checkForTable("ex") == False: 
            CREATE = "CREATE TABLE ex (s2, geonw, geosw, geose, geone, center, name)"
            self.cur.execute(CREATE)

    def checkForTable(self, table_name):
        T=f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
        self.cur.execute(T)
        tlist = self.cur.fetchone()
        if len(tlist) == 1 : 
            return True
        else:
            return False

    def isEntryInDb(self, cellId):
        # ID = 'XXXX'
        T=f"SELECT * FROM ex WHERE s2 == '{cellId}'"
        self.cur.execute(T)
        R = self.cur.fetchone()
        if R is not None:
            return True
        else:
            return False

    def getAll(self ):
        #c = 0
        for i in self.all_cell_IDs:
            fullID = i.id()
            a = '{:016x}'.format(fullID)
            shortHexId = a[0:4]
            if self.isEntryInDb(shortHexId) == False:
                self.getData(i) 
            else:
                self.cout+=1
                print("Already in db")
                #c = c + 1
        #print("C in db %d" %c)

            #self.getData(i)

    def getCells(self, ):
        return list(self.all_cell_IDs)

    def addToDb(self, j):
        print(j)
        j = json.loads(j)
        s2 = j.get('s2')
        geomnw = str(j.get('geom').get('nw')[0]) + ',' + str(j.get('geom').get('nw')[1])
        geomne = str(j.get('geom').get('ne')[0]) + ',' + str(j.get('geom').get('ne')[1])
        geomsw = str(j.get('geom').get('sw')[0]) + ',' + str(j.get('geom').get('sw')[1])
        geomse = str(j.get('geom').get('se')[0]) + ',' + str(j.get('geom').get('se')[1])
        center = str(j.get('geom').get('center')[0]) + ',' + str(j.get('geom').get('center')[1])
        name = j.get('name')
        T = f"INSERT INTO ex VALUES ('{s2}','{geomnw}','{geomsw}','{geomse}','{geomne}','{center}','{name}')"
        self.cur.execute(T)
        self.con.commit()

    def getData( self, cell ):
        #'LatLng: x,y'
        LL = str(cell.to_lat_lng())
        a,b = LL.split(' ')
        LAT,LNG = b.split(',')
        url = "https://ingress-cells.appspot.com/query?lat=%s&lng=%s" %(LAT,LNG)
        response = requests.get(url)
        if response.status_code == 200:
            #{'s2': '310b', 'geom': {'nw': [10.264102, 105.032363], 'sw': [10.189893, 106.534838], 'se': [11.586397, 106.534838], 'ne': [11.670247, 105.032363], 'center': [10.925991, 105.782067]}, 'name': 'AS11-LIMA-05'}
            self.fd.write(response.text + ",")
            self.addToDb(response.text)
            print("Got: " + str(self.cout) + " of: " + str(len(self.all_cell_IDs)))
            self.cout+=1
        else:
            self.ffail.writei(str(cell) + "\n")
            self.ffailCount+=1
        #randomint = random.randint(1, 10)
        #print("Waiting %d seconds before next request" % randomint)
        #time.sleep(randomint)

    def closeFiles(self,):
        self.fd.write("]}")
        self.fd.close()
        self.ffail.close()
        self.con.close()

r = ScrapeAllCellInfo("cell_info.db")
r.getAll()
r.closeFiles()

