#! /usr/bin/python3

import threading
import random
import re
from datetime import datetime
from threading import Lock
from time import sleep
import http.client, urllib.parse
import socket
import json

servers = [('localhost', 8081), ('localhost', 8082), ('localhost', 8083)]
printInterval=321

def make_url(key,n, seq):
    #i=random.randint(1,100)%len(servers)
    return "/v1/keys/{0}?number={1}&seq={2}".format(key, n, seq)

def random_server():
    i=random.randint(1,100)%len(servers)
    return servers[i]

def pick_server(url):
    m=re.search('http://([^/]*)', url)
    if m:
        sx=m.group(1).split(':')
        return (sx[0], int(sx[1]))
    else:
        return ""
    
class CountDownLatch(object):
    def __init__(self, count=1):
        self.count = count
        self.lock = threading.Condition()

    def count_down(self):
        self.lock.acquire()
        self.count -= 1
        if self.count <= 0:
            self.lock.notifyAll()
        self.lock.release()

    def await(self):
        self.lock.acquire()
        while self.count > 0:
            self.lock.wait()
        self.lock.release()
        
class NumberFetcher(object):
    def __init__(self, latch, combinator, seqSeed, seqDist,
                 times=1000, key='monkey.id', printInterval=100):
        self.totalMillis=0
        self.totalTimes=0
        self.key=key
        self.times=times
        self.key=key
        self.latch=latch
        self.combinator=combinator
        self.printInterval=printInterval
        self.conn = None
        self.seqSeed = seqSeed
        self.seqDist = seqDist
        
    def __call__(self):
        try:
            self.execute()
        finally:
            if self.conn:
                self.conn.close()
            self.latch.count_down()

    def connect(self, server, port):
        if self.conn :
            self.conn.close()
        self.conn = http.client.HTTPConnection(server, port)
        
    def execute(self):
        headers = {'X-tans-token':'0BC9F3', 'keep-alive':'true'}
        (server, port)=random_server()
        self.connect(server, port)

        i=0
        seq=self.seqSeed
        url=make_url(self.key, 5, seq)
        while i < self.times:
            t0 = datetime.now()
            try:
                self.conn.request("PUT", url, "", headers)
                r=self.conn.getresponse()
            except http.client.HTTPException as ex:
                print(ex.__class__.__name__)
                (server, port)=random_server()
                self.connect(server, port)
                sleep(0.1)
                continue
            except socket.gaierror:
                print("Can not put " + server)
                (server, port)=random_server()
                self.connect(server, port)
                continue
    
            self.totalMillis+= ((datetime.now() - t0).microseconds)/1000.0
            self.totalTimes+=1

            if r.status == 200 :
                data=r.read().decode('utf-8')
                #print(body)
                j=json.loads(data)
                idRange=j.get('range')
                self.combinator.accept(idRange[0], idRange[1])
                
                if i%self.printInterval == 0:
                    print('%s:%d,%s,%d,%d'%(server, port, self.key, idRange[0], idRange[1]))

                i+=1
                seq += self.seqDist
                url=make_url(self.key, 5, seq)
                    
            elif r.status == 307 :
                target=r.getheader('location')
                (server, port)=pick_server(target)
                #print(server, port)
                self.connect(server, port)
                print("redirect to", target)
                
            else:
                print(r.status)
                sleep(0.1)
                continue

        print('average HTTP duration is %.1f ms'%(self.totalMillis/self.totalTimes))

class RangeCombinator(object):
    def __init__(self):
        self.ranges=[]
        self.lock=Lock()
        self.low=0
        self.high=0
        self.gaps=[]
        self.lastCombineSize = 0
        
    def accept(self, low, high):
        try:
            self.lock.acquire()
            self.ranges.append((low, high))
            if len(self.ranges) - self.lastCombineSize > 1024:
                self.combine(True)
        finally:
            self.lock.release()

    def combine(self, failStop):
        if len(self.ranges) == 0:
            return
        rx=sorted(self.ranges)
        try:
            while len(rx) > 0:
                (a, b)=rx[0]
                if self.high==0:
                    self.low=a
                    self.high=b
                    rx.pop(0)
                elif a == self.high + 1:
                    self.high = b
                    rx.pop(0)
                else:
                    if failStop :
                        self.lastCombineSize = len(rx)
                        return
                    else:
                        r=((self.low, self.high), (a, b))
                        self.gaps.append(r)
                        self.low = a
                        self.high = b
                        rx.pop(0)
        finally:
            self.ranges = rx
        
    def check(self):
        try:
            self.lock.acquire()
            self.combine(False)
            if len(self.gaps) == 0:
                return ""
            else:
                s=""
                for gap in self.gaps:
                    ((a, b), (c, d)) = gap
                    s=s+'(%d, %d) (%d, %d);'%(a, b, c, d)
                return s
        finally:
            self.lock.release()
    
def main():
    threads=[]
    n=15
    combinator=RangeCombinator()
    latch=CountDownLatch(n)
    for i in range(n):
        thread = threading.Thread(target=NumberFetcher(latch, combinator,
                                                       i+n, n, times=10000, printInterval=200),
                                  name='Fetcher%d'%(i), daemon=False)
        thread.start()
        threads.append(thread)
    latch.await()

    checkResult=combinator.check()
    if len(checkResult) == 0:
        print("OK")
    else:
        print(checkResult)
    
if __name__ == '__main__':
    main()
