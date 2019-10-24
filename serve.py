# encoding: utf-8
"""
@author: Sy
@file: index_handler.py
@time: 2019/8/22 11:50
说明：rabbitmq生成者
"""
import pika


class RabbitMQClient(object):
    __instance = None
    channel = None

    def __new__(cls):
        if not cls.__instance:
            cls.__instance = object.__new__(cls)
        return cls.__instance

    def __init__(self):
        self.username = "yehui"
        self.password = "yehui001"
        self.host = "118.24.155.213"
        self.port = 5672
        self.virtual_host = "/vhost1"  # rabbitmq虚拟库
        self.heartbeat = 60  # 链接时长，超时断开链接


    def connection_rabbitmq(self):
        credentials = pika.PlainCredentials('yehui', 'yehui001')
        param = pika.ConnectionParameters(host='118.24.155.213',
                                          port=5672,
                                          virtual_host="/vhost1",
                                          heartbeat=60,
                                          credentials=credentials)
        self.connection = pika.BlockingConnection(param)
        self.channel = self.connection.channel()
        self.channel.confirm_delivery()
        return self.channel


    def publish_message(self, routing_key, body, TTL=0):
        """
        发送消息到指定的队列
        :routing_key  队列名
        :body 消息实体
        :return:
        """
        if not RabbitMQClient.channel:
            RabbitMQClient.channel = self.connection_rabbitmq()
        try:
            if TTL:
                ack = self.channel.basic_publish(exchange='',
                    routing_key=routing_key,
                    body=body,
                    properties=pika.BasicProperties(
                        expiration= str(TTL), # 过期时间
                        delivery_mode=2  # 消息持久化
                    )
                )
            else:
                ack = self.channel.basic_publish(exchange='',
                       routing_key=routing_key,
                       body=body,
                       properties=pika.BasicProperties(
                           delivery_mode=2  # 消息持久化
                       )
                )
        except pika.exceptions.ConnectionClosed:
            print('pika.exceptions.ConnectionClosed')
            RabbitMQClient.channel = self.connection_rabbitmq()
            ack = self.publish_message(routing_key, body, TTL)
        except:
            RabbitMQClient.channel = None
            ack = False
        return ack




