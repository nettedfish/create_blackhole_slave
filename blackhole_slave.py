#!/usr/bin/python26
# -*- coding: utf-8 -*-
# author: yinggang1@staff.sina.com.cn
# function: create blackhole slave of online master
# require MySQLdb for python26. run this script as root. for help, contact yinggang1.
# install python26 and MySQLdb: yum search python26; yum search MySQLdb
# sudo su; yum install python26-mysqldb.x86_64
# date: 2012-09-01

import os
import sys
import logging
import logging.handlers
import subprocess
from optparse import OptionParser
import MySQLdb

log_level = logging.DEBUG
logger_name = "make_blackhole"
log_filename = "/tmp/" + logger_name + ".log"
logger = logging.getLogger(logger_name)
logger.setLevel(log_level)
handler = logging.handlers.RotatingFileHandler(log_filename, maxBytes=1000000000, backupCount=0)
formatter = logging.Formatter("%(asctime)s - [%(levelname)s] - [%(name)s/%(filename)s: %(lineno)d] - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

replica_user = "replica_user"
replica_pass = "replica_pass"
mysql_user = "mysql_user"
mysql_pass = "mysql_pass"
mysql_user_w = "mysql_user_w"
mysql_pass_w = "mysql_pass_w"

def install_standard_mysql(port):
    """安装通用的mysql,默认版本为5.5 引擎采用myisam"""
    return_msg = {}
    cmd_install_mysql = """sudo /etc/dbCluster/mysqlha_install.sh -P %d -v 5.5 -e myisam -d data1""" % (port)
    pipe = subprocess.Popen(cmd_install_mysql, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = pipe.communicate()
    if stderr.strip() != "":
        return_msg["code"] = -1
        return_msg["info"] = "安装mysql发生错误,请检查: %s" % (stderr.strip())
        logger.error(return_msg["info"])
    else:
        return_msg["code"] = 0
        return_msg["info"] = "安装mysql成功！输出信息: %s" % (stdout.strip())
        logger.debug(return_msg["info"])
    return return_msg

def get_server_id_prefix():
    """得到server_id的前缀"""
    cmd_get_prefix = """/sbin/ifconfig eth1 | awk '/inet / {print $2}' | awk -F"." '{printf "%s%s", $3,$4 }'"""
    pipe = subprocess.Popen(cmd_get_prefix, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = pipe.communicate()
    if stderr != "":
        return -1
        logger.error("获取server id前缀出错: %s" % (stderr.strip()))
    else:
        logger.debug("server id前缀: %s" % (stdout.strip()))
        return int(stdout.strip())

def get_master_binlog_format(master_ip, port):
    """获取主库binlog 格式: 主库为s,从库必须为s;主库为r,从库必须为r"""
    try:
        system_connection = MySQLdb.connect(host=master_ip, port=port, passwd=mysql_pass, db='', user=mysql_user, connect_timeout = 5)
    except Exception, e:
        return_msg["code"] = -1
        return_msg["info"] = "系统数据源错误: %s" % str(e)
        logger.debug(return_msg["info"])
        return return_msg
    else:
        system_cursor = system_connection.cursor()
        system_cursor.execute("show global variables like 'binlog_format'")
        f_row = system_cursor.fetchone()
        if f_row is None:
            #5.1之前没有binlog_format这个参数 所有格式都是statement
            binlog_format = "statement"
        else:
            binlog_format = f_row[1].lower()
        return_msg["code"] = 0
        return_msg["info"] = binlog_format
        logger.debug("主库 %s:%d 的binlog_format值为: %s" % (master_ip, port, binlog_format))
        return return_msg

def start_mysqld(port):
    """启动mysqld服务器"""
    return_msg = {}
    cmd_start_mysqld = """/etc/dbCluster/mysqlha_start.sh -P %d >/dev/null 2>&1""" % (port)
    os.system(cmd_start_mysqld)

def get_db_structure_from_master(master_ip, port):
    """从线上拉取所有的表和库结构"""
    print "清理本地系统库下的所有表: mysql performance_schema zjmdmm..."
    #因所有账号均没有drop权限,所以只能采用rm 的方式drop库和表,对myisam表完全可行
    cmd_rm_origin_myisam_tables = """cd /data1/mysql%d/ && rm -rf mysql performance_schema zjmdmm""" % (port)
    os.system(cmd_rm_origin_myisam_tables)
    logger.debug("清理本地系统相关表")

    print "flush 本地系统相关表..."
    cmd_close_opened_tables = """mysql -u%s -p%s -h 127.0.0.1 --port=%d -e "flush tables;" """ % (mysql_user, mysql_pass, port)
    os.system(cmd_close_opened_tables)
    logger.debug("flush tables")

    print "从线上导入所有库和表的结构..."
    #general_log slow_log表的数据默认不会被写入binlog 所以丢弃
    cmd_mysqldump = """mysqldump -u%s -p%s -h %s --port=%d --skip-lock-tables --ignore-table=mysql.general_log --ignore-table=mysql.slow_log --add-drop-table=FALSE  -d --all-databases|tee /tmp/structure.sql|mysql -u%s -p%s -h 127.0.0.1 --port=%d >>%s 2>&1""" % (mysql_user, mysql_pass, master_ip, port, mysql_user_w, mysql_pass_w, port, log_filename)
    logger.debug("导入表结构的命令为: %s" % (cmd_mysqldump))
    os.system(cmd_mysqldump)
    logger.debug("从线上导入所有库和表的结构成功")

    print "修改除系统表之外的所有表引擎为blackhole. 请等待..."
    cmd_alter_table_engine = """mysql --skip-column-names -u%s -p%s --host=127.0.0.1 --port=%d information_schema -e 'select concat("alter table ",table_schema,".",table_name, " engine=blackhole;") from tables where table_schema not in ("zjmdmm","mysql","information_schema","performance_schema");'|mysql -n -u%s -p%s --host=127.0.0.1 --port=%d >>%s 2>&1""" % (mysql_user_w, mysql_pass_w, port, mysql_user_w, mysql_pass_w, port, log_filename)
    os.system(cmd_alter_table_engine)
    logger.debug("修改除系统表之外的所有表引擎为blackhole")

def sync_data_with_master(master_ip, port):
    """从主库获取所需的数据:权限库,监控库等,其余库不需要数据.全程加锁"""
    return_msg = {}
    try:
        system_connection = MySQLdb.connect(host=master_ip, port=port, passwd=mysql_pass, db='', user=mysql_user, connect_timeout = 5)
    except Exception, e:
        return_msg["code"] = -1
        return_msg["info"] = "系统数据源错误,无法访问数据库! %s" % str(e)
        logger.debug(return_msg["info"])
        return return_msg
    else:
        #获取所有需要同步数据的表,一个列表 目前就是mysql zjmdmm, 可以灵活添加
        cmd_get_all_myisam_tables = """mysql --skip-column-names -u%s -p%s --host=%s --port=%d information_schema -e 'select concat(table_schema,".",table_name) from tables where table_schema in ("mysql","zjmdmm")'""" % (mysql_user, mysql_pass, master_ip, port)
        logger.debug("得到所有myisam相关表的命令为: %s" % (cmd_get_all_myisam_tables))
        pipe1 = subprocess.Popen(cmd_get_all_myisam_tables, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout1, stderr1 = pipe1.communicate()
        if stderr1.strip() != "":
            return_msg["code"] = -1
            return_msg["info"] = "获取myisam 列表出错."
            logger.debug(return_msg["info"])
            return return_msg

        lock_tables_sql = "lock tables "
        for table in stdout1.strip().splitlines():
            #general_log 和 slow_log 两个表无法被锁住,所以排除
            if table == "mysql.general_log" or table == "mysql.slow_log":
                continue
            lock_tables_sql = lock_tables_sql + table + " read,"
        lock_tables_sql = lock_tables_sql.rstrip(",") + ";"
        logger.debug("获取读锁的sql语句为: %s" % (lock_tables_sql))
        system_cursor = system_connection.cursor()
        system_cursor.execute("begin;")
        system_cursor.execute(lock_tables_sql)
        logger.debug("获取表上的读锁成功,准备开始导入数据")

        #在上述myisam表被锁住的时候,导出所有的表数据
        #只需要mysql和zjmdmm的数据
        cmd_sync_data = """mysqldump -u%s -p%s -h %s --port=%d --lock-tables=FALSE --ignore-table=mysql.general_log --ignore-table=mysql.slow_log --add-drop-table=FALSE -t --databases mysql zjmdmm|tee /tmp/data.sql|mysql -u%s -p%s -h 127.0.0.1 --port=%d""" % (mysql_user, mysql_pass, master_ip, port, mysql_user_w, mysql_pass_w, port)

        pipe2 = subprocess.Popen(cmd_sync_data, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout2, stderr2 = pipe2.communicate()
        if stderr2.strip() != "":
            return_msg["code"] = -1
            return_msg["info"] = "获取myisam表数据出错:%s 执行的命令为:%s" % (stderr2.strip(), cmd_sync_data)
            logger.debug(return_msg["info"])
            return return_msg
        else:
            logger.debug("导入mysql zjmdmm数据到本地数据库成功")

        system_cursor.execute("show master status;")
        offset_row = system_cursor.fetchone()
        if offset_row is None:
            return_msg["code"] = -1
            return_msg["info"] = "show master status 出错,请检查日志!"
            logger.debug(return_msg["info"])
            return return_msg
            
        file = offset_row[0]
        pos = offset_row[1]
        logger.debug("show master status获取主库binlog位置: %s %s" % (file, pos))

        #导出数据,获取binlog pos后即可释放锁
        system_cursor.execute("unlock tables;")
        system_cursor.execute("commit;")
        sql_change_master_to = """CHANGE MASTER TO 
            MASTER_HOST='%s',
            MASTER_USER='%s',
            MASTER_PASSWORD='%s',
            MASTER_PORT=%d,
            MASTER_LOG_FILE='%s', 
            MASTER_LOG_POS=%d,
            MASTER_CONNECT_RETRY=10;""" % (master_ip, replica_user, replica_pass, int(port), file, int(pos))
        logger.debug("change master to语句:  %s" % (sql_change_master_to))
        cmd_change_master = """mysql -u%s -p%s --host=127.0.0.1 --port=%d -e "%s start slave;select sleep(3);show slave status \G"|grep -i -E 'slave_(io|sql)_running'|awk '{print $2}'""" % (mysql_user, mysql_pass, port, sql_change_master_to)
        pipe3 = subprocess.Popen(cmd_change_master, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout3, stderr3 = pipe3.communicate()
        if stderr3 != "":
            return_msg["code"] = -1
            return_msg["info"] = "建立主从关系,启动同步出错,请检查:%s" % (stderr3.strip())
            logger.error(return_msg["info"])
            return return_msg
        else:
            logger.debug("搭建blackhole从库成功.输出为:%s " % (stdout3.strip()))
            io, sql = stdout3.strip().splitlines()
            if io == "Yes" and sql == "Yes":
                return_msg["code"] = 0
                return_msg["info"] = "io 和 sql线程状态均为Yes, 同步正常!"
                print "  " + return_msg["info"]
                logger.debug(return_msg["info"])
                return return_msg
            else:
                return_msg["code"] = -1
                return_msg["info"] = "同步状态有误:io 线程状态: %s sql线程状态: %s" % (io, sql)
                logger.error(return_msg["info"])
                return return_msg

def generate_mysqld_config(mysql_port, server_id_prefix, binlog_format):
    """生成blackhole mysqld的配置文件"""
    return_msg = {}
    cmd_gen_cfg = """
mysql_port=%d
server_id_prefix=%d
binlog_format=%s

cat <<EOF
[mysqld]
port                    = ${mysql_port}
user                    = my${mysql_port}
datadir                 = /data1/mysql${mysql_port}
tmpdir                  = /dev/shm
socket                  = /tmp/mysql${mysql_port}.sock
key_buffer_size         = 128M
max_allowed_packet      = 16M
join_buffer_size        = 16M
sort_buffer_size        = 16M
read_buffer_size        = 16M
read_rnd_buffer_size    = 32M
myisam_sort_buffer_size = 128M
max_tmp_tables          = 256
tmp_table_size          = 128M
max_heap_table_size     = 128M
thread_cache            = 64
thread_concurrency      = 32
max_connections         = 2048
max_user_connections    = 1024
max_connect_errors      = 99999999
myisam_repair_threads   = 1
myisam-recover          = DEFAULT
expire_logs_days        = 10
read_only               = 1
skip-slave-start        = 1
skip-name-resolve       
character-set-server    = utf8
log-slave-updates=1
binlog-format = ${binlog_format}
net_write_timeout=3600
wait_timeout=28800
net_read_timeout=28800
slave_net_timeout=28800
interactive_timeout=28800
default-storage-engine          = blackhole
skip-innodb
server-id                       = ${server_id_prefix}${mysql_port}
log-bin                         = mysql-bin
relay-log                       = relay-bin
log-error                       = error.log

[client]
user                            = user
password                        = pass
port                            = ${mysql_port}
socket                          = /tmp/mysql${mysql_port}.sock
no-auto-rehash
character_set_client            = utf8

[myisamchk]
key_buffer                      = 64M
sort_buffer_size                = 32M
read_buffer                     = 16M
write_buffer                    = 16M
EOF""" % (mysql_port, server_id_prefix, binlog_format)
    pipe1 = subprocess.Popen(cmd_gen_cfg, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout1, stderr1 = pipe1.communicate()
    if stderr1.strip() != "":
        return_msg["code"] = -1
        return_msg["info"] = "生成mysqld配置文件发生错误,请检查:%s" % (stderr1.strip())
        logger.error(return_msg["info"])
    else:
        logger.debug("生成配置文件,得到的标准输出为:%s" % (stdout1.strip()))
        pipe2 = subprocess.Popen("grep . >/data1/mysql%d/my%d.cnf" % (mysql_port, mysql_port), stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE, shell=True)
        stdout2, stderr2 = pipe2.communicate(input=stdout1)
        if stderr2 != "":
            return_msg["code"] = -1
            return_msg["info"] = "保存配置项到配置文件发生错误,请检查: =%s=" % (stderr2.strip())
            logger.error(return_msg["info"])
            return return_msg
        else:
            return_msg["code"] = 0
            return_msg["info"] = "保存配置项到配置文件成功, 标准输出为: =%s=" % (stdout2.strip())
            logger.debug(return_msg["info"])
            return return_msg

if __name__ == "__main__":
    parser = OptionParser(version="%prog 1.0", usage="example: %prog -f 10.75.10.10 -P 3628")
    return_msg = {}

    parser.add_option("-f", "--master-ip", help="master ip address here", type="string", dest="ip")
    parser.add_option("-P", "--master-port", help="master port here", type="int", dest="port")

    (options, args) = parser.parse_args()

    if not options.ip:
        parser.print_help()
        parser.error("请输入主库ip地址")
    if not options.port:
        parser.print_help()
        parser.error("请输入主库端口")

    logger.debug("输入的ip地址为:%s 输入的端口为:%s" % (options.ip, options.port))
    port = options.port
    master_ip = options.ip

    #保证不会把线上已有的数据库干掉
    if os.path.exists("/data1/mysql%s/" % str(port)):
        print "本地已经有端口 %s 存在? 如果要继续安装,请先remove!" % (port)
        sys.exit()

    #1.安装mysql
    return_msg = install_standard_mysql(port)
    if return_msg["code"] == -1:
        print "安装mysql发生错误,请检查日志 tail -f %s" % (log_filename)
        sys.exit()
    else:
        print "安装mysql成功,继续..."

    server_id_prefix = get_server_id_prefix()
    return_msg = get_master_binlog_format(master_ip, port)
    if return_msg["code"] == 0:
        binlog_format = return_msg["info"]
        if binlog_format == "statement":
            print "##Warning: 主库binlog格式为statement, 从库binlog格式自动设置为statement"
    else:
        print return_msg["info"]
        sys.exit()

    #2.生成bh的配置文件
    return_msg = generate_mysqld_config(port, server_id_prefix, binlog_format)
    if return_msg["code"] == -1:
        print "生成blackhole mysql配置文件发生错误,请检查日志 tail -f %s" % (log_filename)
        sys.exit()
    else:
        print "生成blackhole配置文件成功,继续..."

    #3.启动mysqld
    start_mysqld(port)

    #4.获取表结构
    get_db_structure_from_master(master_ip, port)

    #5.同步数据
    return_msg = sync_data_with_master(master_ip, port)
    if return_msg["code"] == -1:
        print "搭建blackhole从库,同步过程中出错,请检查日志 tail -f %s" % (log_filename)
        sys.exit()
    else:
        print "搭建blackhole从库成功!"
