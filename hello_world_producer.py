#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import pika, sys

exchange_name = "test.exchange.direct.liqifei"
queue_name = "test.queue.liqifei"

def main():
    # 认证
    credentials = pika.PlainCredentials(username="weiqing", password="weiqing")
    conn_params = pika.ConnectionParameters(host="192.168.49.59", credentials=credentials)

    # 打开TCP连接
    conn_broker = pika.BlockingConnection(conn_params)
    # 创建一个信道
    channel = conn_broker.channel()
    # 开启发布确认模式
    channel.confirm_delivery()
    # 声明交换器
    exchange = channel.exchange_declare(exchange=exchange_name,exchange_type="direct", durable=True)
    # 声明并绑定队列
    queue = channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(exchange=exchange_name, queue=queue_name)

    # 持久化的消息确认
    msg_props = pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE)
    # 非持久化的消息确认
    # msg_props = pika.BasicProperties(delivery_mode=pika.spec.TRANSIENT_DELIVERY_MODE)

    message = sys.argv[1]
    confirm = channel.basic_publish(body=message, exchange=exchange_name, routing_key=queue_name, properties=msg_props)
    if confirm:
        print("publish success")
    else:
        print("message lost")
    # for i in range(5000):
    #     channel.basic_publish(body="测试%d" % i, exchange=exchange_name, routing_key=queue_name, properties=msg_props)

    conn_broker.close()


def confirm_handler(frame):
    if type(frame.method) == spec.Confirm.SelectOk:
        print("in confirm mode")
    elif type(frame.method) == spec.Basic.Nack:
        print("message lost")
    elif type(frame.method) == spec.Basic.Ack:
        print("confirm received")

if __name__ == "__main__":
    main()