# coding=gbk
import os
import sys
sys.path.append('../')
from JobProducer_bi import *
import datetime
from optparse import OptionParser

HADOOP_HOME = '/home/users/wangxiao10/workspace/soft_install/hadoop-client-stoff/'

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-d', '--date', action='store', type="string", dest='date_all', help='example: 20130121-20130225')
    parser.add_option('-m', '--module', action='store', dest='module', help='which exp')
    parser.add_option('-r', '--run', action='store', type="string", dest='run', help='measure-import')
    parser.add_option('-t', '--create_table', action='store_true', dest='create_table', help='delete old table and create table while you import data at first time')

    (opt, args) = parser.parse_args(sys.argv)
    if opt.date_all is None or opt.module is None:
        parser.print_help()
        sys.exit(0)
    
    module = opt.module
    
        
    date_org = opt.date_all.split('-')
    if len(date_org) == 1:
        date_org.append(date_org[0])
    d1 = datetime.datetime.strptime(date_org[0], '%Y%m%d')
    d2 = datetime.datetime.strptime(date_org[1], '%Y%m%d')
    day_num = (d2 - d1).days
    date = []
    for i in range(day_num + 1):
        date_next = datetime.timedelta(days=i)
        date_new = d1 + date_next
        date.append(date_new.strftime('%Y%m%d'))
    
    cmd = opt.run.split("-")
    run = {}
    for r in cmd:
        run[r] = 1

    if opt.create_table:
        del_table = 1
    else:
        del_table = 0
	
    n = 0

    for d in date:
	if run.get('measure'):
            stoff_path = '/ps/ubs/wangxiao10/AB_Test_mergelog/' + module + '/' + d
            job = HadoopJobProducer_bi()
            job.setJobName("mergelog_"+module+'_'+d)
            job.setMapstr("./run/python2.6/bin/python mapper.py " + module)
            job.setReducestr("./run/python2.6/bin/python reducer.py")
            #job.setReducestr("cat")
            job.setJobPriority("VERY_HIGH")
            #job.addFile(["mapper.py",  module +"/worker.py", module +"/confidence.list", module +"/url.txt","utils.py", module +"/filter.py", "reducer.py","log_parser.so"])
            job.addFile(["mapper.py",  module +"/worker.py", module +"/confidence.list","utils.py", module +"/filter.py", "reducer.py","log_parser.so"])
            job.addInput('mergelog-v2-rank-all', d)
            job.setOutput(stoff_path)
            job.setReduceNum(100)
            job.addOtherInfomation("-cacheArchive \"hdfs://szwg-stoff-hdfs.dmop.baidu.com:54310/ps/ubs/jinyingming/python2.6.tar.gz#run\"")
            #job.addOtherInfomation("-jobconf mapred.input.pathFilter.class=org.apache.hadoop.fs.FilePathFilter")
            job.addOtherInfomation("-jobconf mapred.filter.file.path=\"my_path.txt\"")
            job.addOtherInfomation("-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner")
            job.addOtherInfomation("-jobconf mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator")
            job.addOtherInfomation("-jobconf stream.num.map.output.key.fields=2")
            job.addOtherInfomation("-jobconf num.key.fields.for.partition=1")
            job.addOtherInfomation("-jobconf mapred.text.key.comparator.options=\"-k1,2\"")
            job.addOtherInfomation("-jobconf mapred.job.map.capacity=1000")
            cmd = HADOOP_HOME + "hadoop/bin/hadoop fs -rmr " + stoff_path
            print "EXECUTING: " + cmd
            os.system(cmd)
            print job.produceHadoopStr()
            os.system(job.produceHadoopStr())
        
        if run.get('import'):
            if n==0 and del_table:
                cmd = "python import.py "+module+" " + d + " "  + " create"
                print "EXECUTING: "+cmd
                os.system(cmd)
            else:
                cmd = "python import.py "+module+" " + d + " " + " dis"
                print "EXECUTING: "+cmd
                os.system(cmd)
	n += 1
			



