from elasticsearch import Elasticsearch
import json
from urllib3.exceptions import NewConnectionError
from socket import error as SocketError, timeout as SocketTimeout


doc1={'author': 'Kautilya','text': 'the book written','color': 'purple ','date':'monday','username':'names'}
doc2={'author': 'krishna','text': 'the coder','color': 'red ','date':'tuesday','username':'names1'}
doc3={'author': 'tom jerry','text': 'cartoon serial','color': 'blue ','date':'wednesday','username':'names2'}
doc4={'author': 'google','text': 'search for anything','color': 'yellow ','date':'thursday','username':'names3'}
doc5={'author': 'karthavya','text': 'building technologies','color': 'green ','date':'friday','username':'names4'}
doc6={'author': 'yahoo','text': 'the old search engine','color': 'white','date':'saturday','username':'names5'}
doc7={'author': 'youtube','text': 'popular video player','color': 'black ','date':'sunday','username':'names6'}
doc8={'author': 'quora','text': 'the knowledge sharing platform','color': 'orange ','date':'today','username':'names7'}
doc9={'author': 'java','text': 'complex programming language','color': 'white ','date':'weekend','username':'names8'}
doc10={'author': 'python','text': 'easy one and popular','color': 'yellow is red and blue ','date':'yesterday','username':'names9'}
doc11={'author': 'root ','text': 'need password','color': 'green is purple and black ','date':'holiday','username':'names10'}
es = Elasticsearch()
doc=(doc1,doc2,doc3,doc4,doc5,doc6,doc7,doc8,doc9,doc10,doc11)
try:
    es = Elasticsearch (['http://karthavya:karthavya@localhost:9200'])
    k=0
    for i in doc:

        res = es.index (index="test", doc_type='tweet', id=k, body=i)
        k=k+1
except(ConnectionRefusedError,NewConnectionError,ConnectionError,Exception,ConnectionError,SocketError,SocketTimeout):
    print("not found elastic cluster")

es.indices.refresh(index="test")
res = es.indices.get_mapping()
rgf=res['test']['mappings']['tweet']['properties']
fiel=list(rgf.keys())
t='y'
while t=='y':
    print(" enter the query need1.\n1.one word search,two word search,multifield search\n2.specific field search search\n3.and search\n4.full string match")
    x=0
    while True:
        try:
            x = int(input("Please enter a number: "))
            if x in range(1,5):
                break
            else:
                raise ValueError

        except ValueError:
            print("Oops!  That was no valid number.  Try again...")

    if x==4:
        str1 = input ("enter the query: ")
        st ='"'+str1+'"'
        y = fiel[x]
        bod = {"query": {"query_string": {"query": st}}}
        bod = json.dumps (bod)
        res = es.search (index="test", body=bod)
        print (res)
        t= input("what to search another y/n:- ")
    if x==3:
        print("and field")
        k = 0
        str1 = input ("enter the query: ")
        lis=str1.split(" ")
        y = fiel[x]
        st= lis[0]+" AND "+lis[1]
        bod ={"query": {"query_string" :{"fields" : fiel,"query" : st }}}
        bod = json.dumps (bod)
        res = es.search(index="test", body=bod)
        print( res)
        t = input ("what to search another y/n :-")
    if x==2:
        print("search in the specific field")
        k = 0
        for i in fiel:
            print(str(k)+"."+i)
            k+=1
        x = int (input ("select the fields you want to search"))
        str1=input("enter the query")
        y=fiel[x]
        bod = {"query": {"term": {y:str1}}}
        bod = json.dumps (bod)
        res = es.search(index="test", body=bod)
        print( res)
        t = input ("what to search another y/n :-")
    if x==1:
        str2=input ("enter the query: ")
        bod ={"query": {"multi_match": {"query":  str2,"type":   "most_fields","fields": fiel}}}
        bod = json.dumps (bod)
        print (bod)
        res = es.search(index="test", body=bod)
        print( res)
        t = input ("what to search another y/n :-")








