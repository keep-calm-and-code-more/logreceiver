import pika
import sys
import os
import time
from pathlib import Path


class LogConfirmer(object):
    FILENAME = "log.txt"

    def __init__(self):
        self.cache_dict = {}
        self.already = set()
        try:
            os.remove(self.FILENAME)
            Path(self.FILENAME).touch()
        except OSError:
            pass

    def add(self, tid, node, body):
        if tid not in self.already:
            self.cache_dict.setdefault(tid, {}).setdefault(node, []).append(body)
            self.checkAndSave(tid)

    def checkAndSave(self, tid):
        vl = list(map(len, self.cache_dict[tid].values()))
        cl = [int(i >= 5) for i in vl]
        if sum(cl) > 0:
            pickOne = cl.index(1)
            self.__save__(tid, pickOne)

    def __save__(self, tid, pickOne):
        batchlog = list(self.cache_dict[tid].values())[pickOne]
        if len(batchlog) < 5:
            raise Exception
        if len(batchlog) > 5:
            batchlog = batchlog[0:5]
        timed_batchlog = ["{}, {}".format(time.time(), i) for i in batchlog]
        for i in timed_batchlog:
            with open(self.FILENAME, "a") as f:
                f.write(i + "\n")
        self.cache_dict.pop(tid, None)
        self.already.add(tid)


previous_tid = ""
batchlog = []
lc = LogConfirmer()


def callback(ch, method, properties, body):
    global previous_tid
    global batchlog
    global lc
    body = body.decode("utf-8")
    # print(">>>>>>", body)
    log_line = body.split("|")
    if len(log_line) <= 2:
        # print(body)
        return
    _, tid, node = list(map(str.strip, log_line[0:3]))
    lc.add(tid, node, body)
    # i = "{}, {}\n".format(time.time(), body)
    # with open("log.txt", "a") as f:
    #     f.write(i)


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='shimlog')
    channel.basic_consume(queue='shimlog', on_message_callback=callback, auto_ack=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
