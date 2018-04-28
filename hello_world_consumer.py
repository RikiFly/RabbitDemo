#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import pika

exchange_name = "test.exchange.direct.liqifei"
queue_name = "test.queue.liqifei"
consumer_tag = "consumer_tag"

def main():
    # 认证
    credentials = pika.PlainCredentials(username="weiqing", password="weiqing")
    conn_params = pika.ConnectionParameters(host="192.168.49.59", credentials=credentials)

    # 打开TCP连接
    conn_broker = pika.BlockingConnection(conn_params)
    # 创建一个信道
    channel = conn_broker.channel()
    # 声明交换器
    exchange = channel.exchange_declare(exchange=exchange_name,exchange_type="direct", durable=True)
    # 声明并绑定队列
    queue = channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(exchange=exchange_name, queue=queue_name)

    channel.basic_consume(consumer_callback=consumer, queue=queue_name, consumer_tag=consumer_tag)
    channel.start_consuming()


# 用于处理接收到的消息
def consumer(channel, method, header, body):
    # 确认消息, 确认后队列会删除这个消息
    channel.basic_ack(delivery_tag=method.delivery_tag)
    print("=======================")
    print(channel)
    print(method)
    print(method.delivery_tag)
    print(header)
    print(body)
    print(bytes.decode(body))
    print("=======================")
    if bytes.decode(body) == "quit":
        # 停止消费(同时关闭信道和连接)并退出
        channel.basic_cancel(consumer_tag=consumer_tag)
        channel.stop_consuming()
        print("end")

if __name__ == "__main__":
    main()