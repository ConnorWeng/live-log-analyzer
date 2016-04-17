import re
from datetime import date
from MySQLdb import connect
from settings import MY_HOST, MY_USER, MY_PASS, MY_DB


class Analyzer51zwd(object):
    def __init__(self):
        self.db = connect(host = MY_HOST,
                          user = MY_USER,
                          passwd = MY_PASS,
                          db = MY_DB)
        self.cursor = self.db.cursor()
        self.today = date.today().isoformat()
        self.stores = dict()
        self.regex = re.compile(r'/goods/(?P<goods_id>\d+)')

    def yjsc_count(self):
        today_log_file = './logs/' + self.today + '.log'
        fd = open(today_log_file, 'r')
        for line in fd:
            parts = line.split('||')
            url = parts[3]
            m = self.regex.search(url)
            if m:
                goods_id = m.groupdict()['goods_id']
                goods_id = goods_id[len(goods_id) - 1]
                self.cursor.execute("select store_id from ecm_goods where goods_id = " + goods_id)
                rs = self.cursor.fetchone()
                if rs:
                    store_id = rs[0]
                    print "store %s +1" % store_id
                    if self.stores.has_key(store_id):
                        self.stores[store_id] = self.stores[store_id] + 1
                    else:
                        self.stores[store_id] = 1

    def save(self):
        for store in self.stores:
            rs = self.cursor.execute("insert into analyze_yjsc(store_id, date, count) values (%s, %s, %s)" % (store, self.today.replace('-', ''), self.stores[store]))
            print rs
        self.db.commit()
        self.cursor.close()
        self.db.close()


if __name__ == '__main__':
    a = Analyzer51zwd()
    a.yjsc_count()
    a.save()
