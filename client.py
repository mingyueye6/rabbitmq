# encoding: utf-8
"""
@author: Sy
@file: index_handler.py
@time: 2019/8/22 11:50
说明：rabbitmq消费者
"""

import json
import re

import pika
import pymysql
import redis
import requests
import base64

url = "https://api.jpush.cn/v3/push"

credentials = pika.PlainCredentials('yehui', 'yehui001')
param = pika.ConnectionParameters(host='118.24.155.213',
                                  port=5672,
                                  virtual_host="/vhost1",
                                  credentials=credentials)
connection = pika.BlockingConnection(param)
channel = connection.channel()


# 打开数据库连接
db = pymysql.connect(host="118.24.155.213",
        user="xiaochengfu_test",
        passwd="JRLDACGaHMGwdbFc",
        db="xiaochengfu_test",
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor) #封装成字典
cur = db.cursor()
# pool2 = redis.ConnectionPool(host='118.24.155.213', port=6379, password='yehui', db=1)
# r2 = redis.StrictRedis(connection_pool=pool2)


def executesql(sql, params=None, methon='query'):
    # sql语句列表 params参数列表 methon方法列表
    if not params:
        params = []
    results = ''
    try:
        # 使用cursor()方法获取操作游标
        cur.execute(sql, params)  # 执行sql语句
        if methon == 'query':
            results = cur.fetchall()  # 获取查询的所有记录
        elif methon == 'add':
            results = cur.lastrowid  # 新插入的id
        elif methon =='update' or 'delete':
            results = cur.rowcount  # 受影响行数
        db.commit()
    except Exception as err:
        print(err)
        db.rollback()
        results = -1
    return results


def executesqls(sql_list, params_list):
    # sql语句列表 params参数列表
    results = ''
    try:
        for sql, params in zip(sql_list, params_list):
            # 使用cursor()方法获取操作游标
            cur.execute(sql, params)  # 执行sql语句
            results = cur.rowcount
        db.commit()
    except Exception as err:
        print(err)
        db.rollback()
        results = -1
    return results


def takeout_order(order_osn, order_type):
    print(order_osn)
    # 订单超时未支付，自动取消
    if order_type == 1:
        sql = '''SELECT id,user_id FROM xcf_online_order WHERE status=0 AND order_type=1 AND order_osn=%s'''
        datas = executesql(sql, [order_osn])
        if datas:
            sql = '''UPDATE xcf_online_order SET status=6 WHERE id=%s'''
            executesql(sql, [datas[0]['id']])
    # 配送超时，提醒商户
    elif order_type == 2:
        pass


def reserve_order(order_osn, order_type):
    print(order_osn)
    # 预约订单，超时过期
    if order_type == 3:
        sql = '''SELECT id,user_id FROM xcf_online_order WHERE order_type=2 AND status in (1,4) AND order_osn=%s'''
        datas = executesql(sql, [order_osn])
        if datas:
            sql = '''UPDATE xcf_online_order SET status=6 WHERE id=%s'''
            executesql(sql, [[datas[0]['id']]])
    elif order_type == 4:
        sql = '''SELECT orders.id,users.registration_id FROM xcf_online_order orders LEFT JOIN xcf_users users ON orders.user_id=users.user_id
        WHERE orders.order_type=2 AND orders.status=4 AND orders.order_osn=%s'''
        datas = executesql(sql, [order_osn])
        if datas:
            sql = '''UPDATE xcf_online_order SET remind=1 WHERE id=%s'''
            executesql(sql, [datas[0]['id']])


def callback(ch, method, properties, body):
    msg = json.loads(body.decode().replace("'", '"'))
    order_osn = msg.get('order_osn')  # 订单号
    print(order_osn)
    # order_type = msg.get('order_type')  # 分类，1支付超时 2配送超时 3订单过期 4红包退款
    # if re.search('^W\d{18}$', order_osn):
    #     takeout_order(order_osn, order_type)
    # elif re.search('^Y\d{18}$', order_osn):
    #     reserve_order(order_osn, order_type)

    # 如果处理成功，则调用此消息回复ack，表示消息成功处理完成。
    channel.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(  # 消费消息
    callback,  # 如果收到消息，就调用callback函数来处理消息
    queue='consume',  # 你要从那个队列里收消息
)
channel.start_consuming()

# # 消息放在rabbitmq中
# msg = {"order_osn": order_osn, "order_type": 3}
# tm = (order_time_ - datetime.datetime.now()).seconds + 1800
# if not serve_rabbit.publish_message('reserve_order', str(msg), tm * 1000):
#     return returnJson(self, 0, msg='下单失败，请重试')

