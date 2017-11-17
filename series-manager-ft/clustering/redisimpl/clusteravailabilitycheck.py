
import redis
import threading
import logging
import time
import json

from threading import Timer

logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)

class ClusterAvailabilityCheck(threading.Thread):

    def __init__(self, redis, server_id, url, _queue_, presence_interval):

        threading.Thread.__init__(self)

        self.redis = redis
        self.server_id = server_id
        self.channel_name = "cluster_management_channel"
        self._queue_ = _queue_
        self.presence_interval = presence_interval
        self.pubsub = self.redis.pubsub()
        self.pubsub.subscribe(self.channel_name)
        self.bootstrap = True
        self.servers = dict()
        self.ordinal = -1
        self.cluster_availability = None
        self.server_url = url

        self.timer = Timer(2*self.presence_interval, self.end_of_bootstrap )
        self.timer.start()
        self.tab=dict()

    def end_of_bootstrap(self):
        self.bootstrap = False;
        max_ordinal = -1
        for ordinal in self.servers.keys() :
            if ordinal > max_ordinal :
                max_ordinal = ordinal

        if -1 == max_ordinal :
            self.ordinal = 0
            logging.info("%s is master", self.server_id)

        else :
            #logging.info("max_ordinal = %s", max_ordinal)
            self.ordinal = 1 + max_ordinal
            logging.info("%s is backup ----x", self.server_id)

        if self.cluster_availability :
            self.cluster_availability.set_ordinal(self.ordinal)
            self.cluster_availability.publishClusterPresence()

    def set_cluster_availability(self, cluster_availability):
        self.cluster_availability = cluster_availability



    def is_master(self):
        if self.bootstrap :
            return False

        for ordinal in self.servers.keys():
            if self.ordinal > ordinal :
                return False

        return True

    def get_instance_urls(self):
        urls = list()
        for ordinal in self.servers.keys():
            status = self.servers[ordinal]
            logging.info("%s", status)
            urls.append(status['url'])
        return urls

    def get_master_url(self):
        _ordinal_ = -1
        for ordinal in self.servers.keys():
            if ordinal == self.ordinal :
                continue

            if _ordinal_ == -1 :
                _ordinal_ = ordinal
            else :
                if ordinal < _ordinal_ :
                    _ordinal_ = ordinal

        status = self.servers[_ordinal_]
        return status['url'];


    def run(self):

        #logging.info("Cluster availability check thread routine running.")

        while True :
            message = self.pubsub.get_message()
            if message :
                # logging.info("RECEIVED = %s", message)
                if message['data'] == 1 :
                    None
                else :
                    # print ("MESSAGE:"+str(message['data']))
                    # d = json.dumps(message['data'])
                    status = json.loads(message['data'])
                    self.tab[status['ordinal']]=status['timestamp_epoch']

                    #  print (str(self.tab))

                    check_server_is_dead(self)

                    if self.server_id == status['id'] :
                        #logging.info("Fr['data']om Myself = %s", status['id'])
                        None
                    else :
                        logging.info("Server ID = %s Ordinal = %d on cluster", status['id'], status['ordinal'])
                        self.servers[status['ordinal']] = status


                    #if _status_['id'] == self.server_id :
                    #    logging.info("Server ID = %s (myself)", _status_['id'])

                #if _status_['id'] == self.server_id :
                #    logging.info("RECEIVED = %s (MYSELF)", message)
                time.sleep(0.5)
        #self.end_of_bootstrap(self)


        #for item in self.pubsub.listen():
        #    logging.info("%s", item)


def check_server_is_dead(self):
    for ordinal,timestamp in self.tab.items():
        #   print str("TIMESTAMP"+str(timestamp)+" ORDINAL"+str(ordinal))
        if(timestamp+1.5*self.presence_interval<time.time()):
            del self.tab[ordinal]
            del self.servers[ordinal]
            display_status(self)
    print ("=> Servers list status:"+str(self.tab))



def display_status(self):
    if self.is_master():
        print ("Je suis le nouveau master !")
    else:
        print ("Je suis backup")
