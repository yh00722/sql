from traceback import format_exc
import eventlet
import pymysql
from DBUtils.PooledDB import PooledDB
from pymysql.converters import escape_string


class MysqlHelper(object):

    

    def __init__(self, host, user, password,
                 database, port,
                 charset):

        self.pool = PooledDB(
            creator=pymysql,  # 使用链接数据库的模块
            maxconnections=6,  # 连接池允许的最大连接数，0和None表示不限制连接数
            mincached=2,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
            maxcached=5,  # 链接池中最多闲置的链接，0和None不限制
            maxshared=3,
            # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
            blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
            maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
            setsession=[],  # 开始会话前执行的命令列表。如：["set datestyle to ...", "set time zone ..."]
            ping=0,
            # ping MySQL服务端，检查是否服务可用。# 如：0 = None = never, 1 = default = whenever it is requested, 2 = when a cursor is created, 4 = when a query is executed, 7 = always
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset=charset,
            autocommit='True'
        )

    def connect(self):
        """连接
        # 通过创建数据库连接池来创建数据库连接对象的方式解决数据库性能问题
        """
        conn = self.pool.connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        return conn, cursor

    def __edit(self, sql, params=None):
        while True:
            try:
                conn, cursor = self.connect()
                count = 0
                with eventlet.Timeout(30, False):
                    conn.ping(reconnect=True)
                    cursor.execute(sql, params)
                    conn.commit()
                    count += 1

                if count != 0:
                    return True
                else:
                    print('数据库挂载错误,将重试')

            except Exception as e:
                # print(format_exc())
                print(e)
                return False

    def execute(self, sql, params=None):
        """执行sql语句操作"""
        return self.__edit(sql, params=params)

    def escape(self, m_str):
        """
        mysql转义
        :param str:
        :return:
        """
        new_str = pymysql.escape_string(m_str)
        return new_str

    # 查
    def get_all(self, sql, params=()):
        """获取所有相关的数据"""
        try:
            conn, cursor = self.connect()
            cursor.execute(sql, params)
            r = cursor.fetchall()
            # coloumns = [row[0] for row in self.cursor.description]
            # result = [[item for item in row] for row in self.cursor.fetchall()]
            # r = [dict(zip(coloumns, row)) for row in result]

        except Exception as e:
            r = []
            print(e)

        return r

    def get_last_id(self):
        """
        获取数据插入后的id
        :return:
        """
        result = self.get_all('SELECT LAST_INSERT_ID();')
        return result

    # 增
    def mysql_insert(self, table, **kwargs):
        """
        插入
        :param table:
        :param kwargs:
        :return:
        """
        table = table
        keys = ','.join(kwargs.keys())
        values = ','.join(['%s'] * len(kwargs))
        sql = 'INSERT INTO {table}({keys})values ({values})'.format(table=table, keys=keys, values=values)
        result = self.execute(sql, tuple(kwargs.values()))
        print('mysql_insert:', result)
        return result

    def file_insert(self, table, **kwargs):
        table = table
        keys = ','.join(kwargs.keys())
        values = ','.join(['%r'] * len(kwargs))
        sql = 'INSERT ignore INTO {table}({keys})values ({values});'.format(table=table, keys=keys, values=values)
        with open('%s.sql' % table, 'a', encoding='utf-8') as f:
            f.write(sql % tuple(kwargs.values()) + '\n')
        print('file_insert_success')

    # 改
    def update(self, sql):
        """
        sql = UPDATE <表名> SET 字段 1=值 1 [,字段 2=值 2… ] [WHERE 子句 ]
        :param table:
        :param kwargs:
        :return:
        """
        result = self.execute(sql)
        print('update:', result)

    #  存在情况下进行修改
    def mysql_insert_or_update(self, table, **kwargs):
        """
        插入更新：数据值不能为整数，否则报错
        :param table: 表名
        :param kwargs:
        :return:
        """

        keys = ','.join(kwargs.keys())
        values = ','.join(['{}'] * len(kwargs))
        sql = 'INSERT INTO {table}({keys})values ({values})ON DUPLICATE KEY UPDATE '.format(table=table, keys=keys,
                                                                                            values=values)
        update = ','.join(['{key}'.format(key=key) + '= {}' for key in kwargs])
        sql += update

        # 避免数据库因为单双引号报错的问题,做个正形判断
        values = ["'%s'"% escape_string(i) if (type(i) != int and type(i) != float) else i for i in list(kwargs.values())]
        sql = sql.format(*tuple(values) * 2)+';'
        result = self.execute(sql)
        print('mysql_insert_or_update:', result)
        return result

    def file_insert_or_update(self, table, **kwargs):
        """
        插入更新：数据值不能为整数，否则报错
        :param table: 表名
        :param kwargs:
        :return:
        """
        table = table
        keys = ','.join(kwargs.keys())
        values = ','.join(['{}'] * len(kwargs))
        sql = 'INSERT INTO {table}({keys})values ({values})ON DUPLICATE KEY UPDATE '.format(table=table, keys=keys,
                                                                                            values=values)
        update = ','.join(['{key}'.format(key=key) + '= {}' for key in kwargs])
        sql += update

        # 避免数据库因为单双引号报错的问题,做个正形判断
        values = ["'%s'"%escape_string(i) if type(i) != int else i for i in list(kwargs.values())]

        sql = sql.format(*tuple(values) * 2)+';'

        with open('%s.sql' % table, 'a', encoding='utf-8') as f:
            f.write(sql + '\n')

        return 'Successful'


