import os
import requests
import sys
from collections import Counter

def search(sIp):

    # get all the videos
    base_url = "http://{}:8080/v5".format(sIp)
    resp = requests.get("{}/videos".format(base_url))
    respJson = resp.json()

    total = 0
    statuses = []
    for video in respJson['videos']:
        videoId = video['id']
        status = video['status']
        #requests.post("{}/videos/{}:resume".format(base_url, videoId))
        total += 1
        statuses.append(status)

    while True:
        
        if 'nextPageToken' not in respJson:
            break
        nextPageToken = respJson["nextPageToken"]
        resp = requests.get("{}/videos?pageToken={}".format(base_url,nextPageToken))
        respJson = resp.json()

        for video in respJson['videos']:
            videoId = video['id']
            status = video['status']
            #requests.post("{}/videos/{}:resume".format(base_url, videoId))
            total += 1
            statuses.append(status)

    print('status:', Counter(statuses))
    print('total:', total)

if __name__ == '__main__':
    sIp = sys.argv[1]
    search(sIp)
