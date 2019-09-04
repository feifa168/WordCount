package com.ft.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
    public static class Map extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, IntWritable> {
        /**
         * Map处理逻辑
         * Map输入类型为<key, value> -> <LongWritable, Text>
         * Map输出类型为<单词, 出现次数> -> <Text, IntWritable>
         **/

        // 每个单词出现次数都为1
        private final static IntWritable one = new IntWritable(1);
        // 解析出来每个单词用Text类型存储
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value,
                        OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {
            /**
             * @Description: 重写map函数
             * @param: [key, value, output, reporter]
             * @return: void
             **/
            // Text类型转换成String类型
            String line = value.toString();
            // 分词器，将一行文本切分成多个单词
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                // 取出单词并进行输出
                word.set(tokenizer.nextToken());
                output.collect(word, one);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements
            Reducer<Text, IntWritable, Text, IntWritable> {
        /**
         * Reduce处理逻辑
         * Reduce输入类型为<Text, IntWritable>
         * Reduce输出类型为<Text, IntWritable>
         **/

        @Override
        public void reduce(Text key, Iterator<IntWritable> values,
                           OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {
            /**
             * @Description: 重写reduce函数
             * @param: [key, values, output, reporter]
             * @return: void
             **/
            // 统计词频并输出结果
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        // 配置信息
        JobConf conf = new JobConf(WordCount.class);
        String[] ioArgs=new String[]{"sort_in","sort_out"};
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        conf.setJobName("wordcount");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        // 文件路径信息
        //FileInputFormat.setInputPaths(conf, new Path("hdfs://192.168.199.105:9000/user/hadoop/input/"));
        FileInputFormat.setInputPaths(conf, new Path("hdfs://localhost:9000/user/hadoop/input/"));
        //FileOutputFormat.setOutputPath(conf, new Path("/Users/wangyutian/code/java/hadoop/result/wordCount"));
        FileOutputFormat.setOutputPath(conf, new Path("hdfs://localhost:9000/user/hadoop/output/wordCount2"));


        //加载默认配置
        //Configuration conf=new Configuration(true);
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

        // 执行
        JobClient.runJob(conf);
    }
}
