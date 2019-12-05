import sys 
import logging 
import pymysql 
import random 
import json 
rds_host  = "music.cx6fdt3ccnvy.us-east-1.rds.amazonaws.com" 
name = "admin" 
password = "Bison_01" 
db_name = "music" 
 
 
 
 
def lambda_handler(event, context): 
    try: 
            conn = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, connect_timeout=5) 
 
    except pymysql.MySQLError as e: 
            print("ERROR: Unexpected error: Could not connect to MySQL instance.") 
            print(e) 
            sys.exit() 
 
 
    artist = event.get('queryStringParameters') 
    fdict={} 
    print("SUCCESS: Connection to RDS MySQL instance succeeded") 
    #If not artist information passed then call database to get all artist 
    if artist is None: 
        sql = "SELECT artist FROM artist"  
        with conn.cursor() as cursor: 
            cursor.execute(sql) 
            result = cursor.fetchone() 
            while result: 
                fdict[result[0]]=result[0] 
                result = cursor.fetchone() 
        print (fdict) 
        data = json.dumps(fdict) 
        return {"statusCode": 200,"headers": {"Access-Control-Allow-Origin":'*'}, "body":  data }        
             
    array = list(artist.values()) 
    #build list of artist  
    listToStr = ','.join(['"'+str(elem)+'"' for elem in array])  
 
    #loop through each artist    
    for key in artist: 
        print(artist[key]) 
        art=artist[key] 
 
        with conn.cursor() as cursor: 
            #Get region and scoring for artist from the database 
            tlist = []   
            sql = "SELECT region,scoring_matrix FROM artist where artist=%s "  
            cursor.execute(sql,art) 
            result = cursor.fetchone() 
            #If not match for artist then we have a problem and skip 
            if result is None: 
                print("Artist is not in database, can not get region or scoring value...skipping") 
            else: 
                print(result) 
                region =result[0] 
                        sm = result[1] 
                sql2 = "not in (%s)" % listToStr 
                sql = "SELECT artist, song_info FROM artist where region=%s and scoring_matrix=%s and artist " +sql2  
                cursor.execute(sql,(region,sm)) 
                result = cursor.fetchone() 
 
                #Could not find a match for region and scoring matrix so just using scoring matrix 
                if result is None: 
                    print("NO Match for region and scoring") 
                    sql2 = "not in (%s)" % listToStr 
                    sql = "SELECT artist FROM artist where scoring_matrix=%s and artist " +sql2          
                    cursor.execute(sql,(sm)) 
                    result = cursor.fetchone() 
     
                #Completed printing 
                tlist = []   
                done=bool(False)     
                while result is not None: 
                    tlist.append(result) 
                    result = cursor.fetchone() 
                size= len(tlist) 
                while  not done: 
                    pick= random.randint(0,size-1)       
                    #print(tlist) 
                    #print (pick) 
                    #print(tlist[pick][0]) 
                    if fdict.get(tlist[pick][0]) is None: 
                        fdict[tlist[pick][0]]=tlist[pick] 
                        done=True 
                    else: 
                        print("duplicate") 
    print("------list is done---------") 
    print fdict 
    return {"statusCode": 200,"headers": {"Access-Control-Allow-Origin":'*'} ,"body":  json.dumps(fdict) } 
 
def main(): 
  event = { 
  "a1": "Jay-Z", 
  "a2": "Travis Scott", 
  "a3": "Lil Kim" 
, "a4" : "Lili Pump", "a5" : "Lili Uzi Vert"} 
  lambda_handler(event,"test") 
if __name__== "__main__": 
    main() 
