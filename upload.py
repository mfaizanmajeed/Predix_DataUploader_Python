
import time
import datetime
import os
import predix.app
import predix.data.timeseries
from glob import glob
pth = "K:\\"
app = predix.app.Manifest('manifest.yml')

# We will use Time Series to ingest data
ts = predix.data.timeseries.TimeSeries()


def CheckOldData():
    try:
        with open("Default_Store.csv" , "r") as file:
                lines = file.readlines()
        for i in lines:
            data=i.split(";")
            ts.queue(data[0],value=data[1],timestamp=data[2])
            ts.send()
            print (data)
        os.remove("Default_Store.csv")
    except Exception:
        print ("Or NO Internet :(")
        print ("Old Data Not Found! :)")
        
                

def upload():
    x=glob(pth+"Sample Inquiry*.txt")
    with open(x[0] , "r") as file:
            lines = file.readlines()
    data = []
    for i in lines:
        data.append(i.split("\t"))
    for i in range(1,len(lines)-1):
        time_stamp= data[i][0] + " " +data[i][1].replace(' PM','') 
        time_stamp= time_stamp.replace(' AM','')   
        unix_timestamp=time_stamp 
        unix_timestamp = int(time.mktime(datetime.datetime.strptime(unix_timestamp, "%Y-%m-%d %H:%M").timetuple()))

        for k in range (2,len(data[0])):
            data[0][k]=data[0][k].strip()
            try:
                if k==2:
                    if data[i][k]=='':
                        data[i][k]=0
                    else:
                        data[i][k]=1
                    #print("")
                else:
                    data[i][k]=float(data[i][k])
            except Exception:    
                data[i][k]='-9999.9999'
            try:
                ts.queue("LAB:"+data[0][k], value=data[i][k],timestamp=unix_timestamp*1000)
                print("LAB:"+data[0][k], data[i][k],unix_timestamp*1000)
                ts.send()
            except Exception:
                print ("No internet")
                with open("Default_Store.csv" , "a") as file:
                    file.write("LAB:"+data[0][k]+";"+str(data[i][k])+";"+str(unix_timestamp*1000)+"\n")
        time.sleep(1)
    for i in x:
        os.remove(i)

        
CheckOldData()
upload()
