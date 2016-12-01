# coding=utf8
'''
Created on 2014-12-09

@author: wangxiao10
'''
import sys
module = sys.argv[1]
sys.path.append(module)
import os
import mysql.connector
import types
import mapper

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print >> sys.stderr, '''
        Usage: %s sugTag 20140404 create
        ''' % sys.argv[0]
        sys.exit(1)

    table = sys.argv[1]
    date = sys.argv[2]
    
    # download HDFS file to loacl
    HDFS_path = '/ps/ubs/wangxiao10/AB_Test_mergelog/' + table + '/' + date + '/part-000[5,6,7,8,9]*'
    local_file_path = '/home/work/wangxiao10/.tmp/AB_Test_mergelog/' + table + '/' + date + '/pv_result.txt'
    os.system('rm -rf /home/work/wangxiao10/.tmp/AB_Test_mergelog/' + table + '/' + date)
    os.makedirs('/home/work/wangxiao10/.tmp/AB_Test_mergelog/' + table + '/' + date)

    cmd = 'hadoop fs -cat '+HDFS_path +' > ' + local_file_path
    print cmd
    os.system(cmd)
        
    mapper.init()
    total_keys = mapper.KEYS
    total_kvs = mapper.KVS
    drop_sql = "DROP TABLE IF EXISTS " + table
    id_sql = "`id` int (10) NOT NULL AUTO_INCREMENT,"
    pri_sql = "PRIMARY KEY (`ID`)"
    create_sql = "CREATE TABLE IF NOT EXISTS `" + table + '` (`id` int (10) NOT NULL AUTO_INCREMENT,' + ','.join('`' + key + '` int(10) NOT NULL' if (type(total_kvs[key]) is types.IntType) else '`' + key + '` varchar(250) DEFAULT NULL' for key in total_keys) + " ,PRIMARY KEY (`ID`) ,KEY `date` (`date`),KEY `query` (`query`),KEY `sid` (`sid`)) ENGINE=InnoDB DEFAULT CHARSET=utf8"
    in_sql = "INSERT INTO " + table + " (" + ','.join(key for key in total_keys) + ") VALUES (" + ','.join('%s' if (type(total_kvs[key]) is types.IntType) else '%s' for key in total_keys) + ")"
    cnx = mysql.connector.connect(user='root',
                                  password='123456',
                                  host='cq01-rdqa-pool002.cq01.baidu.com',
                                  port='8001',
                                  database='wangxiao10',
                                  #host='st01-dy-exp02.st01.baidu.com',
                                  #port='3998',
                                  #database='ui',
                                  charset='utf8',
                                  autocommit=False,
                                  buffered=True
                                  )
    cursor = cnx.cursor()
    # create new table
    if sys.argv[3] == 'create':
        print 'start creat table ' + table
        cursor.execute(drop_sql)
        cursor.execute(create_sql)
        cnx.commit()
        
    data_list = []
    flag = 0
    f = open(local_file_path, 'r')
    num = 0
    cmd = 'cat ' + local_file_path + ' | wc -l'
    total_lines = int(os.popen(cmd).read())
    pos = 0

    print 'start import data, about ' + str(total_lines) + ' lines ' + 'need to import'
    format_failue = 0

    while True:
        l = f.readline()
        if not l:
            break
        if len(l.split('\t')) < 10 :
            continue
        l = l.strip()
        data = tuple(l.strip().split('\t'))

        data_list.append(data)
        pos += 1
        if len(data_list) > 5000 :
            num = num + 1
            cursor.executemany(in_sql, data_list)
            del data_list[:]
            if num == 10 :
                num = 0
                cnx.commit() 
                print str(pos * 100.0 / total_lines) + '% finished, ' + str(pos) + ' lines imported'
    f.close()
    cursor.executemany(in_sql, data_list)
    cnx.commit() 
    cursor.close()
    cnx.close()
    print "total " + str(format_failue) + " failed!"
    print 'all data import to db sucess!'
