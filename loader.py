__author__ = 'alex'
from couchbase import Couchbase
from couchbase.exceptions import CouchbaseError
import json
import sys
import re
'''
Json does not have syntax to define unordered bucket. So '{{' or '(' cannot be changed into json
FacebookUsers: key id
FacebookMessages: key message-id
TwitterUsers: key screen-name
TweetMessages: key tweetid
'''

def L(a, b, c):


    cb=Couchbase.connect(bucket=b,host='getafix-macmini.ics.uci.edu',port=8091,password='',timeout=36000)
    filename = a+'.adm'

    file=open(filename,'r')
    outfile=open(a+'.json','w')
    #outfile.write('[\n')
    line=file.readline()
    r=0;
    while line:
        r+=1
        if r%100000==0:
            print(r)
        if(r>-1):

            timezone=-1
            while line.find('datetime',timezone+1,len(line))>=0:
                timezone=line.find('datetime',timezone+1,len(line))
                line=line[0:timezone+29]+'Z'+line[timezone+29:]

            while line.find('int64')>=0:
                i=line.find('int64')
                z=line.find('\")',i)
                line=line[0:z-1].replace('int64(\"','',1)+line[z-1:].replace('\")','',1)

            line=line.replace('{{','[')
            line=line.replace('}}',']')
            while line.find('(\"')>=0 and line.find('\")')>=0:
                i=line.find('(\"')
                z=line.rfind(':',0,i)
                line=line.replace('(\"','\":\"',1)
                line=line.replace('\")','\"}',1)
                line=line[0:z+1]+'{\"'+line[z+1:]
            #outfile.write(line)
            data=json.loads(line)
            while 1==1:
                try:
                    result=cb.set(str(data[c]),data)
                    break
                except CouchbaseError as e:
                    aa=1

        line=file.readline()

    #outfile.write(']')
    file.close()
    outfile.close()
    '''
    filename = a+'.json'
    data = json.load(open(str(filename)))
    print('next')
    for i in range(0, len(data)):
        key = data[i][c]
        if i%100000==0:
            print(i)
        outfile = open(b+'/' + str(key) + '.json', 'w')
        json.dump(data[i], outfile)
        outfile.close()
    return

    filename = a+'.json'
    file=open(filename,'r')
    line=file.readline()
    while line:
        if line.find('\"'+c+'\"')>=0:
            i=line.find('\"'+c+'\"')
            z=line.find(':',i)
            ze=line.find(',',i)
            name=line[z+2:ze]
            outfile = open(b+'/' + name + '.json', 'w')
            outfile.write(line)
            outfile.close()
            line=file.readline()
            if line:
                line=line[1:]
            else:
                break
    return
    '''

L('fb_message','FacebookMessages','message_id')

L('fb_users','FacebookUsers','id')

#L('twu','TwitterUsers','screen-name')

L('tw_message','TweetMessages','tweetid')


'''
    filename = a+'.json'
    file=open(filename,'r')
    line=file.readline()
    while line:
        if line.find(c)>=0:
            i=line.find('\"'+c+'\"')
            z=line.find(':',i)
            ze=line.find(',',i)
            name=line[z+1:ze]
            print(name)
            outfile = open(b+'/' + name + '.json', 'w')
            outfile.write(line)
            outfile.close()
'''


