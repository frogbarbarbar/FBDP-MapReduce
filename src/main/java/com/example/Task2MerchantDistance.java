package com.example;

import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task2MerchantDistance {
    public static class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
        // 输入：<行偏移量, 文本行>
        // 输出：<商户ID, 距离\t用户ID>
        private Text outKey = new Text();
        private Text outVal = new Text(); // 将携带距离和用户ID
        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString();
            if (line == null || line.trim().isEmpty()) return;   // 跳过空行
            String low = line.toLowerCase();
            if (low.startsWith("user_id") || low.contains("user_id,")) return;  // 跳过表头，即第一行

            String[] f = line.split(",", -1);   // 分割CSV行，保留空字段
            try {
                if (f.length >= 7) {
                    String user = f[0].trim();    // 用户 ID
                    String merchant = f[1].trim();   // 商户 ID
                    String distanceStr = f[4].trim();   // 距离
                    if (merchant.isEmpty() || user.isEmpty()) return;
                    if (distanceStr == null || distanceStr.isEmpty() || distanceStr.equalsIgnoreCase("null")) return; // 忽略 NULL 的距离值
                    if (!distanceStr.matches("\\d+")) return;

                    int d = Integer.parseInt(distanceStr);
                    if (d < 0 || d > 10) return;

                    outKey.set(merchant);
                    outVal.set(d + "\t" + user);
                    ctx.write(outKey, outVal);    // 输出：<商户ID, 距离\t用户ID>
                }
            } catch (Exception ex) {
                // ignore malformed lines
            }
        }
    }

    public static class Reducer2 extends Reducer<Text, Text, Text, Text> {
        // 对每个商户，统计在不同距离上唯一用户的数量
        // 输入：<商户ID, [距离\t用户ID](列表)>
        // 输出：<商户ID, 各距离用户数（制表符分隔）>
        @Override
        protected void reduce(Text key, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException {
            // maintain an array of HashSet for distances 0..10
            @SuppressWarnings("unchecked")
            HashSet<String>[] sets = new HashSet[11];   // 为距离0到10创建11个HashSet，用于去重存储用户ID
            for (int i=0;i<=10;i++) sets[i] = new HashSet<>();
            
            for (Text v : vals) {   // 处理每个值
                String[] p = v.toString().split("\t", 2);
                if (p.length < 2) continue;
                int d = Integer.parseInt(p[0]);  // 距离
                String user = p[1];              // 用户ID
                sets[d].add(user);  // 将用户ID添加到对应距离的HashSet中
            }
            // 构建输出字符串
            StringBuilder sb = new StringBuilder();
            for (int i=0;i<=10;i++) {
                if (i>0) sb.append("\t");
                sb.append(sets[i].size());   // 将每个距离的唯一用户数量追加到字符串
            }
            ctx.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Task2MerchantDistance <input> <output>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task2 Merchant Distance");
        job.setJarByClass(Task2MerchantDistance.class);
        job.setMapperClass(Mapper2.class);
        job.setReducerClass(Reducer2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0])); // offline input
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }    
}
