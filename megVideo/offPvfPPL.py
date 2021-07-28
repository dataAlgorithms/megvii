import time
import base64
import requests
import os
import sys
import json
from multiprocessing import Pool
import fnmatch
import re


videos = {
 "11010816001320016173_1_20191217090000_20191217093000-5times-10minCut.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/11010816001320016173_1_20191217090000_20191217093000-5times-10minCut.ts    ",
"ch02m_20191025151641t9153-5times-10minCut-afterVF.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/ch02m_20191025151641t9153-5times-10minCut-afterVF.ts",
"ped-veh_mid_9_c15-10minCut.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/ped-veh_mid_9_c15-10minCut.ts",
"20200601-1-7-10minCut.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/20200601-1-7-10minCut.ts",
 "20190903113206-10min.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/20190903113206-10min.ts",
"rxsb_汾湖华润万家1F出口_20190609120000_20190609140000-10min-1.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/rxsb_汾湖华润万家1F出口_20190609120000_20190609140000-10min-1.ts",
  "rxsb_汾湖华润万家1F出口_20190609120000_20190609140000-10min-1_1.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/rxsb_汾湖华润万家1F出口_20190609120000_20190609140000-10min-1.ts",
 "rxsb_汾湖华润万家1F出口_20190609120000_20190609140000-10min-1mTo11m.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/rxsb_汾湖华润万家1F出口_20190609120000_20190609140000-10min-1mTo11m.ts",
"rxsb_汾湖华润万家1F出口_20190609120000_20190609140000-10min-1mTo11m_1.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/rxsb_汾湖华润万家1F出口_20190609120000_20190609140000-10min-1mTo11m.ts    ",
 "rxsb_汾湖华润万家1F出口_20190609120000_20190609140000-10min.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/rxsb_汾湖华润万家1F出口_20190609120000_20190609140000-10min.ts",
"rxsb_汾湖华润万家1F出口_20190609120000_20190609140000-10min_1.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/rxsb_汾湖华润万家1F出口_20190609120000_20190609140000-10min.ts",
"xinan_zhongxin-5times-10minCut.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/xinan_zhongxin-5times-10minCut.ts",
"xinan_zhongxin-5times-10minCut_1.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/xinan_zhongxin-5times-10minCut.ts",
"电_横塘路雷劈山市场枪1_27125278_1564541863_1-5times-10minCut.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/电_横塘路雷劈山市场枪1_27125278_1564541863_1-5times-10minCut.ts",
"电_横塘路雷劈山市场枪1_27125278_1564541863_1-5times-10minCut_1.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/电_横塘路雷劈山市场枪1_27125278_1564541863_1-5times-10minCut.ts",
"电_老年公寓门口1_26EB5920_1564541841_1-5times-10minCut.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/电_老年公寓门口1_26EB5920_1564541841_1-5times-10minCut.ts",
 "电_老年公寓门口1_26EB5920_1564541841_1-5times-10minCut_1.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/电_老年公寓门口1_26EB5920_1564541841_1-5times-10minCut.ts",
"电_六狮洲路口_21DA0328_1564541672_1-5times-10minCut.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/电_六狮洲路口_21DA0328_1564541672_1-5times-10minCut.ts",
"电_六狮洲路口_21DA0328_1564541672_1-5times-10minCut_1.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/电_六狮洲路口_21DA0328_1564541672_1-5times-10minCut.ts",
 "电_七星辅星路与龙隐路交叉口枪机1_26EB3AF8_1564541672_1-5times-10minCut.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/电_七星辅星路与龙隐路交叉口枪机1_26EB3AF8_1564541672_1-5times-10minCut.ts",
 "电_七星辅星路与龙隐路交叉口枪机1_26EB3AF8_1564541672_1-5times-10minCut_1.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/电_七星辅星路与龙隐路交叉口枪机1_26EB3AF8_1564541672_1-5times-10minCut.ts",
"舜新路东方广场农业银行门口_20190610225239_20190610230132-5times-10minCut.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/舜新路东方广场农业银行门口_20190610225239_20190610230132-5times-10minCut.ts",
"舜新路东方广场农业银行门口_20190610225239_20190610230132-5times-10minCut_1.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/舜新路东方广场农业银行门口_20190610225239_20190610230132-5times-10minCut.ts",
 "ch01m_20191024102106t224F-5times-10minCut-afterVF.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/ch01m_20191024102106t224F-5times-10minCut-afterVF.ts",
 "ch01m_20191024102106t224F-5times-10minCut-afterVF_1.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/ch01m_20191024102106t224F-5times-10minCut-afterVF.ts",
 "laoniangongyu_menkou-5times-10minCut.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/laoniangongyu_menkou-5times-10minCut.ts",
"ch0006_00000001848000000-10minCut.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/ch0006_00000001848000000-10minCut.ts",
"jinjilu_checkpoint-5times-10minCut.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/jinjilu_checkpoint-5times-10minCut.ts",
 "jinjilu_checkpoint-5times-10minCut_1.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/jinjilu_checkpoint-5times-10minCut.ts",
"longmen_bridge_into_city_checkpoint_1-5times-10minCut.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/longmen_bridge_into_city_checkpoint_1-5times-10minCut.ts",
 "longmen_bridge_into_city_checkpoint_1-5times-10minCut_2.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/longmen_bridge_into_city_checkpoint_1-5times-10minCut.ts",
 "longmen_bridge_into_city_checkpoint_1-5times-10minCut_1.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/longmen_bridge_into_city_checkpoint_1-5times-10minCut.ts",
 "longmen_bridge_into_city_checkpoint_2-5times-10minCut.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/longmen_bridge_into_city_checkpoint_2-5times-10minCut.ts",
"电_电子科大西区门口对面_26EB5F28_1564541850_1-5times-10minCut-1.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/电_电子科大西区门口对面_26EB5F28_1564541850_1-5times-10minCut-1.ts",
 "电_电子科大西区门口对面_26EB5F28_1564541850_1-5times-10minCut-2.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/电_电子科大西区门口对面_26EB5F28_1564541850_1-5times-10minCut-2.ts",
"电_电子科大西区门口对面_26EB5F28_1564541850_1-5times-10minCut.ts": "/ifs/ipu/qa/data/Benchmark/Video/vss/server_VSS/01-video-test-data/load_40_T4/ts/电_电子科大西区门口对面_26EB5F28_1564541850_1-5times-10minCut.ts"
}


def gen_find(filepat="*", top=None):
    '''
    Find all filenames in a directory tree that match a shell wildcard pattern
    '''
    for path, dirlist, filelist in os.walk(top):
        for name in fnmatch.filter(filelist, filepat):
            yield os.path.join(path,name)

def process2Run(sIp, video_url, modelConfig, name, topic, bucket_name):

    # extract feature and add feature into files
    video_urls = [video_url] * 216
    totalLen = len(video_urls)
    print('totalLen:', totalLen)

    per_num = 1000
    for i in range(totalLen//per_num):
        pool = Pool(processes=48)
        for j in range(per_num):
            pool.apply_async(justiceOffline, args=(j+i*per_num, sIp, video_urls, modelConfig, name, topic, bucket_name, ),)

        print("start")
        pool.close()
        pool.join()
        print("done")

    pool = Pool(processes=48)
    for i in range(totalLen//per_num*per_num, totalLen):
        pool.apply_async(justiceOffline, args=(i, sIp, video_urls, modelConfig, name, topic, bucket_name, ), )

    pool.close()
    pool.join()

# base64 to string
def base64ToString(b):
    return base64.b64decode(b).decode('utf-8')

# create topic
def create_topic(sIp, name):
    base_url = "http://{}:8080/v5".format(sIp)

    data = {
        "name": name
    }

    response = requests.post("{}/topics".format(base_url), json=data)
    print('topic_res:', response.text)

# create pubsub
def create_subscription(sIp, topic, video_id, subType="CAPTURED"):

    message = """messageType==\"MESSAGE_TYPE_""" + subType + """\"&&videoId==\"""" + video_id + """\""""
    ISOTIMEFORMAT = '%Y%m%d%H%M%S'

    base_url = "http://{}:8080/v5".format(sIp)

    name = "subscriptionname_for_{}_{}_{}".format(video_id, subType, str(time.strftime(ISOTIMEFORMAT)))
    data = {
            "name": "subscriptionname_for_{}_{}_{}".format(video_id, subType, str(time.strftime(ISOTIMEFORMAT))),
            "topicName": topic,
            "messageAttributesFilter": message,
            "acknowledgeTimeoutSeconds": 30
    }
    print("subrequestdata:{}".format(data))
    resp = requests.post("{}/subscriptions".format(base_url), json=data)
    print('订阅数据：{}'.format(resp.content))
    return name

# get subpub message
def getSubpubMess(subscription_name):

    wait_time = 10
    count = 5000
    base_url = "http://{}:8080/v5".format(sIp)

    resp = requests.get("{}/subscriptions/{}/messages?maxWaitTimeSeconds={}&maxMessages={}".format(base_url,subscription_name,wait_time,count))
    print('getSubpubMess:', resp.json())
    if resp.json() != '[]':
        resMsgs = resp.json()['receivedMessages']
        for resMsg in resMsgs:
            ack_id =  resMsg["ackId"]
            warnData = resMsg["message"]["data"]
            warnStr = base64ToString(warnData)
            print('warnStr:', warnStr)
            data = {"ackIds": [ack_id]}
            ask = requests.post("{}/subscriptions/{}/messages:acknowledge".format(base_url, subscription_name),
                                             json=data)
    return resp
 
# create bucket
def create_bucket(sIp, name):
    '''
    创建bucket，多个视频可以公用一个
    :param name:bucket的name
    :return:
    '''
    base_url = "http://{}:8080/v5".format(sIp)

    data = {
        "name": name,
        "replica": 1
    }

    bucket_response = requests.post("{}/objectStorage".format(base_url), json=data)
    print('text:', bucket_response.text)

# query bucket
def query_bucket():
                                                                                                                                                          
    response = requests.get("http://{}:8080/v5/objectStorage?filter=bucketType%3D%3D%22DEFAULT%22".format(sIp))
    print(response.text)

# delete bucket
def delete_bucket(name):

    requests.delete("http://{}:8080/v5/objectStorage/{}".format(sIp, name))

# use objectStorage to upload video
def getOfflineVideo(video_path, bucket_name):

    base_url = "http://{}:8080/v5".format(sIp)

    video_file = open(video_path, 'rb')
    upload_result_req = requests.post("{}/objectStorage/{}".format(base_url, bucket_name), data=video_file).content.decode()
    upload_result = json.loads(upload_result_req)
    print(upload_result)

    video_url = base_url + "/objectStorage/" + bucket_name + "/" + upload_result["objectName"]
    print("video_url", video_url)

    return video_url

# get the online video
def getOnlineVideo(video_path):

    video_name = os.path.basename(video_path)
    rtsp_url = "rtsp://10.122.130.33:12000/{}".format(video_name)
    return rtsp_url

# json to modelConfig
def json2ModelConfig(jsonFn):

    with open(jsonFn) as f:
        area_config = json.load(f)
    area_config = base64.b64encode(json.dumps(area_config).encode("utf-8"))
    print('area_config:', area_config)

    return area_config.decode()

# justice online request
def justiceOffline(index, sIp, url, modelConfig, name, topic, bucket='ag-demod-bucket', linkMode='COMPATIBLE',is800w=0):

    url = url[index]

    data = {
 "netgateDeviceId": "",
 "errors": [],
 "name": name,
 "description": "",
 "url": url,
 "pipelineSwitches": {
  "lowLatency": False,
  "withTrackStat": False,
  "linkMode": "PVF_THEN_FACE_DETECTION"
 },
 "stateChangeTopic": "",
  "vehicleAnalyzer": {
    "capture": {
      "analyzeOptions": {
        "extractAttributes": True,
        "extractFeature": True
      },
      "cropSetting": {
        "enabled": True,
        "imageSavePath": "",
        "imageTtlSeconds": 0
      },
      "panoramaSetting": {
        "enabled": False,
        "imageSavePath": "",
        "imageTtlSeconds": 0
      },
      "filter": {
        "minHeightPixels": 10,
        "minWidthPixels": 10
      },
      "indexTtlSeconds": 0,
      "kafkaTopic": "default",
      "vehicleGroupId": "",
      "topic": topic,
      "ttlSeconds": 0,
      "wholeTrack": True
    },
    "enable": True
  },
  "nonmotorAnalyzer": {
    "capture": {
      "analyzeOptions": {
        "extractAttributes": True,
        "extractFeature": True
      }
    },
    "enable": True
  },
  "pedestrianAnalyzer": {
    "capture": {
      "analyzeOptions": {
        "extractAttributes": True,
        "extractFeature": True
      }
    },
    "enable": True
  },
 "faceAnalyzer": {
  "monitors": [],
  "alertPolicy": {
   "messageFilterPolicy": "FILTER",
   "messageTypePolicy": "SINGLE"
  },
  "enable": False,
  "capture": {
   "analysisSettings": {
    "eyeStatus": True,
    "minority": True,
    "liveness": False,
    "age": True,
    "mouthStatus": True,
    "gender": True
   },
   "wholeTrack": True,
   "output": {
    "netgateDeviceId": "",
    "outputFeature": False,
    "faceGroupId": "",
    "kafkaTopic": "",
    "topic": topic
   },
   "indexTtlSeconds": "0",
   "includeLowQuality": False,
   "filter": {
    "minWidthPixels": 0,
    "minHeightPixels": 0
   },
   "cropMode": {
    "cropFace": {
     "enabled": True,
     "imageTtlSeconds": "0",
     "imageSavePath": bucket
    },
    "cropFull": {
     "enabled": True,
     "imageTtlSeconds": "0",
     "imageSavePath": bucket
    },
    "cropBody": {
     "enabled": False,
     "imageTtlSeconds": "0",
     "imageSavePath": bucket
    }
   },
   "ttlSeconds": "0"
  }
 }
}
    print('data:', json.dumps(data, indent=1))

    base_url = "http://{}:8080/v5".format(sIp)
    req = requests.post("{}/offlineVideos".format(base_url), data=json.dumps(data))
    print('req:', req.text)
    req = req.json()

    return req['id']

# justice online request
def justiceOnline(index, sIp, url, modelConfig, name, topic, bucket='ag-demod-bucket'):

    url = url[index]

    '''
    data = {
    "url": url,
    "pipelineSwitches": {
        "linkMode": "COMPATIBLE"
    },
    "restartPolicy": "NEVER",
    "demodAnalyzers": [
        {
            "modelConfig": "{}".format(modelConfig),
            "demodType": "A",
            "enable": True,
            "capture": {
                "kafkaTopic": "default",
                "topic":topic,
                "cropSetting": {
                    "enabled": True,
                    "imageTtlSeconds": 10,
                    "imageSavePath":bucket
                }
            }
        }
    ]}
    '''
    data = {
 "netgateDeviceId": "",
 "errors": [],
 "name": name,
 "description": "",
 "url": url,
 "pipelineSwitches": {
  "lowLatency": False,
  "withTrackStat": False,
  "linkMode": "COMPATIBLE"
 },
 "stateChangeTopic": "",
 "restartPolicy": "ALWAYS",
  "vehicleAnalyzer": {
    "capture": {
      "analyzeOptions": {
        "extractAttributes": True,
        "extractFeature": True
      },
      "cropSetting": {
        "enabled": True,
        "imageSavePath": "",
        "imageTtlSeconds": 0
      },
      "panoramaSetting": {
        "enabled": False,
        "imageSavePath": "",
        "imageTtlSeconds": 0
      },
      "filter": {
        "minHeightPixels": 10,
        "minWidthPixels": 10
      },
      "indexTtlSeconds": 0,
      "kafkaTopic": "default",
      "vehicleGroupId": "",
      "topic": topic,
      "ttlSeconds": 0,
      "wholeTrack": True
    },
    "enable": False
  },
  "nonmotorAnalyzer": {
    "capture": {
      "analyzeOptions": {
        "extractAttributes": True,
        "extractFeature": True
      }
    },
    "enable": False
  },
  "pedestrianAnalyzer": {
    "capture": {
      "analyzeOptions": {
        "extractAttributes": True,
        "extractFeature": True
      }
    },
    "enable": False
  },
 "faceAnalyzer": {
  "monitors": [],
  "alertPolicy": {
   "messageFilterPolicy": "FILTER",
   "messageTypePolicy": "SINGLE"
  },
  "enable": True,
  "capture": {
   "analysisSettings": {
    "eyeStatus": True,
    "minority": True,
    "liveness": False,
    "age": True,
    "mouthStatus": True,
    "gender": True
   },
   "wholeTrack": True,
   "output": {
    "netgateDeviceId": "",
    "outputFeature": False,
    "faceGroupId": "",
    "kafkaTopic": "",
    "topic": topic
   },
   "indexTtlSeconds": "0",
   "includeLowQuality": False,
   "filter": {
    "minWidthPixels": 0,
    "minHeightPixels": 0
   },
   "cropMode": {
    "cropFace": {
     "enabled": True,
     "imageTtlSeconds": "0",
     "imageSavePath": bucket
    },
    "cropFull": {
     "enabled": True,
     "imageTtlSeconds": "0",
     "imageSavePath": bucket
    },
    "cropBody": {
     "enabled": False,
     "imageTtlSeconds": "0",
     "imageSavePath": bucket
    }
   },
   "ttlSeconds": "0"
  }
 }
}

    print('data:', json.dumps(data, indent=1))

    base_url = "http://{}:8080/v5".format(sIp)
    req = requests.post("{}/videos".format(base_url), data=json.dumps(data))
    print('req:', req.text)
    req = req.json()

    return req['id']

def run(sIp):

    bucket_name = "888888"
    json_path = "/ifs/ipu/qa/data/DemodJustice/json/action_video.ts.json"

    num = 0
    maxNum = 100

    import time

    for video in videos:
        video = videos[video].strip()
        create_bucket(sIp, bucket_name)
        videoUrl = getOfflineVideo(video, bucket_name)  
        video_path = videoUrl
        print('video_path:', video_path)
        #create_bucket(sIp, bucket_name)
        video_url = video_path 
        modelConfig = json2ModelConfig(json_path)
        name = os.path.basename(video_path)
        topic = "mess123"
        create_topic(sIp, topic)
        process2Run(sIp, video_url, modelConfig, name, topic, bucket_name) 
        num += 1
        time.sleep(30)

if __name__ == '__main__':
    sIp = sys.argv[1]
    while True:
        run(sIp)
