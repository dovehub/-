import threading
import time
from queue import Queue

import pymongo

try:
    from src.settings import mongo_database, mongo_uri
except ModuleNotFoundError:
    from settings import mongo_database, mongo_uri

class Consumer(threading.Thread):
    def __init__(self, queue, client):
        super().__init__()
        self._queue = queue
        self._client = client

    def run(self):
        while True:
            msg = self._queue.get()
            if isinstance(msg, str) and msg == 'quit':
                break
            collection = self._client[msg['库名']][msg['表名']]
            province_list = list(self._client['数据可视化']['省'].find({}, {'_id': 0}))
            province_list = [p['province'] for p in province_list]
            count_list = collection.aggregate([
                {
                    '$group': {
                        '_id': '$province', 
                        'count': {
                            '$sum': 1
                        }
                    }
                }
            ])
            count_dict = {c['_id']: c['count'] for c in count_list}
            province_count = []
            for province in province_list:
                self._client[mongo_database]['统计'].update_one({'库': msg['库名'], '表': msg['表名'], 'province': province}, {"$set":{'count': count_dict.get(province, 0)}}, upsert=True)
            print(f"{msg}")
        print('已结束')


def producer(max_count=3):
    client = pymongo.MongoClient(mongo_uri)
    db = client[mongo_database]
    count = db['项目入口'].count_documents({'status': 1})
    count = max_count if count >= max_count else count
    queue = Queue(count)
    consumer_list = []
    for _ in range(count):
        consumer = Consumer(queue, client)
        consumer.start()
        consumer_list.append(consumer)
    while 1:
        try:
            for project in db['项目入口'].find({'status': 1}):
                queue.put(project)
            start = time.time()
            time.sleep(1)
            print(time.time() - start)
        except KeyboardInterrupt:
            break
    for _ in range(count):
        queue.put('quit')
    for consumer in consumer_list:
        consumer.join()
    client.close()


if __name__ == "__main__":
    producer()