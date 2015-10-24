# coding=utf-8
from urllib.request import urlopen
import json
import re
import sqlite3
import sys
from pymorphy2 import MorphAnalyzer
from pymorphy2 import units

group_url = 'http://api.vk.com/method/groups.getMembers?group_id={}&offset={}'
wall_url = 'http://api.vk.com/method/wall.get?owner_id={}&offset={}&filter=owner&count=100'
morph = MorphAnalyzer(units=[units.DictionaryAnalyzer, units.UnknAnalyzer])
c = None
conn = None

def fetch_hundred():
    r = c.execute('SELECT word FROM slang ORDER BY amount DESC LIMIT 0, 100')
    print(r)


def find_slang(words):
    for word in words:
        p = morph.parse(word)[0]
        if "UNKN" in p.tag:
            t = (word, 1)
            c.execute('SELECT * FROM slang WHERE word=?', (word,))
            r = c.fetchone()
            if r is None:
                c.execute('INSERT INTO slang(word, amount) VALUES(?, ?)', t)
            else:
                t = (r[2] + 1, word)
                c.execute('UPDATE slang SET amount = ? WHERE word = ?', t)


def parse_posts(user_id):
    global c
    offset = 5000
    total = 0
    while True:
        response = urlopen(wall_url.format(user_id, offset))
        posts = json.loads(response.read().decode("utf-8"))
        if 'error' in posts:
            return
        total = posts['response'][0]
        print("user " + str(user_id))
        print("total: " + str(total))
        to = 100
        if total - to - offset < 0:
            to = total- to - offset
        for i in range(1, to):
            if posts['response'][i]['post_type'] != "copy" and posts['response'][i]['text'] != "":
                post = posts['response'][i]['text']
                words = re.sub(r'[#]\S*', ' ', post)
                words = re.sub(r'[^А-Яа-яЁё]', ' ', words).split()
                find_slang(words)
        offset += 100
        if offset > total:
            break


if __name__ == '__main__':
    group_id = 22798006
    offset = 0
    total = 1000
    try:
        conn = sqlite3.connect('posts.db')
        c = conn.cursor()
        c.execute('''CREATE TABLE slang (
                        id INTEGER PRIMARY KEY,
                        word TEXT,
                        amount INTEGER)''')
        while offset < total:
            response = urlopen(group_url.format(group_id, offset))
            data = json.loads(response.read().decode("utf-8"))
            total = data['response']['count']
            users = data['response']['users']
            print(users)
            for user in users:
                parse_posts(user)
            offset += 1000
    except sqlite3.Error as e:
        print("error: %s" % e.args[0])
        sys.exit(1)
    finally:
        if conn is not None:
            conn.commit()
            fetch_hundred()
            conn.close()
