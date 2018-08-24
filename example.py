import pika

EXCHANGE_NAME = 'test-exchange'
QUEUE_NAME = 'test-queue'

def init_rabbitmq():
  conn = pika.BlockingConnection()
  chan = conn.channel()

  chan.exchange_declare(EXCHANGE_NAME, 'direct')
  chan.queue_declare(QUEUE_NAME, durable=True)
  chan.queue_bind(QUEUE_NAME, EXCHANGE_NAME, "routing.key")

  conn.close()


class  Producer(object):
  def __init__(self, conn):
    self.conn = conn

  def send_message(self, msg, exch, rtg_key):
    chan = self.conn.channel()
    chan.basic_publish(exch, rtg_key, msg)
    chan.close()


class Consumer(object):
  def __init__(self, conn):
    self.conn = conn

  def get_message(self, queue):
    chan = self.conn.channel()
    frame, _, body = chan.basic_get(queue)
    if frame:
      chan.basic_ack(frame.delivery_tag)
    return body


def hello_world():
  init_rabbitmq()

  conn = pika.BlockingConnection()

  p = Producer(conn)

  p.send_message("Hello world!", EXCHANGE_NAME, 'routing.key')
  c = Consumer(conn)

  print(c.get_message(QUEUE_NAME))


if __name__ == "__main__":
  hello_world()
