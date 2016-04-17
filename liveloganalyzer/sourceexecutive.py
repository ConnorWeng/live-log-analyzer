import time
from datetime import date
from os import path
from threading import Thread
from pymongo import Connection
from pymongo.errors import CollectionInvalid, InvalidStringData
from debuglogging import error
from settings import MONGODB_NAME, MAX_COLLECTION_SIZE, SOURCES_SETTINGS


def main():
    for ss in SOURCES_SETTINGS:
        t = Thread(target=run_one, args=(ss,))
        t.start()
        time.sleep(1)


def run_one(settings):
    s = SourceExecutive(settings)
    s.start()


class SourceExecutive(object):
    def __init__(self, settings):
        self.collection = settings['collection']
        self.parser = settings['parser']
        self.source_class = settings['source'][0]
        self.kwargs = settings['source'][1]

    def start(self):
        self.start_source_stream()
        # self.connect_to_mongo()
        self.store_data_local()

    def start_source_stream(self):
        self.source = self.source_class(**self.kwargs)
        self.source.start_stream()

    def connect_to_mongo(self):
        conn = Connection("192.168.99.100", 27017)
        db = conn[MONGODB_NAME]
        self.mongo = db[self.collection]
        try:
            self.mongo = db.create_collection(
                self.collection, capped=True,
                size=MAX_COLLECTION_SIZE * 1048576)
        except CollectionInvalid:
            self.mongo = db[self.collection]

    def store_data(self):
        while True:
            line = self.source.get_line()
            data = self.parser.parse_line(line)
            if data:
                data['server'] = self.source.ssh_params['host']
                try:
                    self.mongo.insert(data)
                except InvalidStringData, e:
                    error('%s\n%s' % (str(e), line))
            else:
                error("%s couldn't parse line:\n%s" % (str(self.parser), line))

    def store_data_local(self):
        today_log_file = './logs/' + date.today().isoformat() + '.log'
        fd = open(today_log_file, 'a')
        while True:
            line = self.source.get_line()
            data = self.parser.parse_line(line)
            if data:
                fd.write("%s||%s||%s||%s||%s||%s||%s\n" % (data['ip'], data['time'], data['type'], data['url'], data['status'], data['refer'], data['ua']))

if __name__ == '__main__':
    main()
