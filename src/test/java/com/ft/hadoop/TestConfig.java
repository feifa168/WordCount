package com.ft.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestConfig {

    @Test
    public void test() {
        //加载默认配置
        Configuration conf=new Configuration(true);
        Path hadoop_mapred=new Path("mapred-site.xml");
        Path hadoop_yarn=new Path("yarn-site.xml");
        conf.addResource(hadoop_mapred);
        conf.addResource(hadoop_yarn);
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
        conf.set("mapreduce.jobtracker.system.dir", "file://data1");
        conf.setInt("data", 10);
        //This parameter will change the same parameter value in the properties
        conf.set("fs.defaultFS", "file///data");
        //conf.set("mapreduce.job.running.map.limit", "2");
        System.out.println("test1=" + conf.get("test1"));
        System.out.println("mapreduce.jobtracker.system.dir=" + conf.get("mapreduce.jobtracker.system.dir"));
        System.out.println("yarn.resourcemanager.admin.address=" + conf.get("yarn.resourcemanager.admin.address"));
        System.out.println("mapreduce.job.running.map.limit=" + conf.get("mapreduce.job.running.map.limit"));
        System.out.println("mapreduce.job.split.metainfo.maxsize=" + conf.get("mapreduce.job.split.metainfo.maxsize"));
        System.out.println("fs.defaultFS=" + conf.get("fs.defaultFS"));
        System.out.println("dfs.namenode.name.dir=" + conf.get("dfs.namenode.name.dir"));
        System.out.println("yarn.application.classpath=" + conf.get("yarn.application.classpath"));
        System.out.println("mapreduce.input.fileinputformat.split.maxsize=" + conf.get("mapreduce.input.fileinputformat.split.maxsize"));
        System.out.println("mapred.max.split.size=" + conf.get("mapred.max.split.size"));
        System.out.println("ok");
    }
}
