package com.example;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Task3CouponAnalysis {
    // JobA - 统计每个优惠券的使用次数、有效间隔次数和总间隔天数
    // JobB - 根据JobA的结果，过滤出使用次数超过总使用次数1%的优惠券，并按照平均使用间隔进行排序

    // ---------------- JobA: Mapper ---------------- 
    public static class JobAMapper extends Mapper<LongWritable, Text, Text, Text> {
        // 输入：<行偏移值，一行的内容>
        // 输出：<优惠券ID, "U\t1\t间隔天数">
        private Text outKey = new Text();
        private Text outVal = new Text();
        private DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyyMMdd");

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString();
            if (line == null) return;
            line = line.trim();
            if (line.isEmpty()) return;
            String low = line.toLowerCase();
            if (low.startsWith("user_id") || low.contains("user_id,")) return; // skip header

            String[] f = line.split(",", -1);
            // offline layout expected: User_id,Merchant_id,Coupon_id,Discount_rate,Distance,Date_received,Date
            if (f.length < 7) return;
            String coupon = f[2].trim();    // 优惠券ID
            if (coupon.isEmpty() || coupon.equalsIgnoreCase("null")) return; // ignore coupon==null

            String date = f[6].trim(); // 使用日期
            String dateReceived = f[5].trim(); // 领取日期

            outKey.set(coupon);
            // 只处理实际使用的记录（date不为空）
            if (!date.isEmpty() && !date.equalsIgnoreCase("null")) {
                // mark a use
                // format: U\t1\tinterval  (interval = -1 if cannot compute)
                long interval = -1L;  // 默认-1表示无法计算
                if (!dateReceived.isEmpty() && !dateReceived.equalsIgnoreCase("null")) {
                    try {
                        LocalDate dr = LocalDate.parse(dateReceived, fmt);
                        LocalDate d = LocalDate.parse(date, fmt);
                        long days = ChronoUnit.DAYS.between(dr, d);
                        if (days < 0) days = 0;  // 确保间隔非负
                        interval = days;
                    } catch (Exception ex) {
                        interval = -1L;  // 解析失败
                    }
                }
                outVal.set("U\t1\t" + interval);
                ctx.write(outKey, outVal);
            }
        }
    }

    // ---------------- JobA: Reducer ----------------
    // Input: key = coupon_id, values = ["U\t1\t<interval>" ...]
    // Output: coupon_id \t use_count \t interval_count \t sum_interval_days
    public static class JobAReducer extends Reducer<Text, Text, Text, Text> {
        // 输入：<优惠券ID, ["U\t1\t间隔天数"](列表)>
        // 输出：<优惠券ID, 使用次数\t有效间隔次数\t总间隔天数>
        @Override
        protected void reduce(Text key, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException {
            long useCount = 0L;   // 总使用次数
            long intervalCount = 0L;   // 可计算间隔的使用次数
            long sumInterval = 0L;    // 总间隔天数
            for (Text v : vals) {
                String s = v.toString();
                String[] p = s.split("\t");
                if (p.length >= 2 && p[0].equals("U")) {
                    try {
                        long c = Long.parseLong(p[1]);
                        useCount += c;   // 累计使用次数
                    } catch (Exception ex) { /* ignore */ }

                    if (p.length >= 3) {
                        try {
                            long interval = Long.parseLong(p[2]);
                            // 只统计有效间隔（interval >= 0）
                            if (interval >= 0) {
                                intervalCount += 1;
                                sumInterval += interval;
                            }
                        } catch (Exception ex) { /* ignore */ }
                    }
                }
            }
            ctx.write(key, new Text(useCount + "\t" + intervalCount + "\t" + sumInterval));
        }
    }
    // 输出示例：
    //     优惠券ID    使用次数    有效间隔次数    总间隔天数
    //       3001        45           40            890



    // ---------------- JobB: Mapper ----------------
    // Input line: coupon_id \t useCount \t intervalCount \t sumInterval
    // We filter by useCount > total_used * 0.01
    public static class JobBMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
        // 输入：JobA的输出，即<优惠券ID, 使用次数\t有效间隔次数\t总间隔天数>
        // 输出：<平均间隔, 优惠券ID\t使用次数\t有效间隔次数\t总间隔天数\t平均间隔>
        private double totalUsed = 1.0;
        private DoubleWritable outKey = new DoubleWritable();
        private Text outVal = new Text();

        @Override
        protected void setup(Context ctx) {
            Configuration conf = ctx.getConfiguration();
            totalUsed = Double.parseDouble(conf.get("total.used", "1.0"));  // 总使用次数
        }

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString();
            if (line == null || line.trim().isEmpty()) return;
            String[] f = line.split("\\t");
            if (f.length < 4) return;
            try {
                String coupon = f[0];
                long useCount = Long.parseLong(f[1]);
                long intervalCount = Long.parseLong(f[2]);
                long sumInterval = Long.parseLong(f[3]);

                if ((double)useCount > totalUsed * 0.01) {  // 只保留使用次数超过全局1%的优惠券
                    if (intervalCount > 0) {   // 只处理能计算平均间隔的优惠券
                        double avg = ((double)sumInterval) / ((double)intervalCount);

                        outKey.set(avg);   // 以平均间隔作为排序键
                        outVal.set(coupon + "\t" + avg);
                        ctx.write(outKey, outVal);
                    } else {
                        // intervalCount == 0 -> cannot compute avg, skip
                    }
                }
            } catch (Exception ex) {
                // ignore malformed
            }
        }
    }

    // ---------------- JobB: Reducer ----------------
    // receives keys (avg) in ascending order; output coupon info lines in that order
    public static class JobBReducer extends Reducer<DoubleWritable, Text, Text, Text> {
        // 输入：<平均间隔, 优惠券ID\t使用次数\t有效间隔次数\t总间隔天数\t平均间隔>
        // 输出：<优惠券ID, 其余统计信息>
        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException {
            for (Text v : vals) {
                // v: coupon \t useCount \t intervalCount \t sumInterval \t avg
                String[] parts = v.toString().split("\\t");
                String coupon = (parts.length > 0) ? parts[0] : "";  // 优惠券ID
                ctx.write(new Text(coupon), new Text(String.valueOf(key.get())));
            }
        }
    }

    // ---------------- Driver ----------------
    // args: <input_offline_csv> <jobA_output_dir> <jobB_output_dir>
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Task3CouponAnalysis <input_offline_csv> <jobA_output> <jobB_output>");
            System.exit(2);
        }
        String input = args[0];
        String jobAOut = args[1];
        String jobBOut = args[2];

        Configuration conf = new Configuration();
        // JobA
        Job jobA = Job.getInstance(conf, "Task3 JobA AggregateUseCounts");
        jobA.setJarByClass(Task3CouponAnalysis.class);
        jobA.setMapperClass(JobAMapper.class);
        jobA.setReducerClass(JobAReducer.class);
        jobA.setMapOutputKeyClass(Text.class);
        jobA.setMapOutputValueClass(Text.class);
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(jobA, new Path(input));
        FileOutputFormat.setOutputPath(jobA, new Path(jobAOut));
        boolean okA = jobA.waitForCompletion(true);
        if (!okA) {
            System.err.println("JobA failed");
            System.exit(1);
        }

        // 读取JobA输出，计算所有优惠券的总使用次数
        long totalUsed = 0L;
        FileSystem fs = FileSystem.get(conf);
        Path outdir = new Path(jobAOut);
        RemoteIterator<LocatedFileStatus> iter = fs.listFiles(outdir, false);
        while (iter.hasNext()) {
            LocatedFileStatus status = iter.next();
            String name = status.getPath().getName();
            if (!name.startsWith("part-")) continue;
            FSDataInputStream in = fs.open(status.getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] f = line.split("\\t");
                if (f.length >= 2) {
                    try {
                        long use = Long.parseLong(f[1]);  // 第2列是使用次数
                        totalUsed += use;
                    } catch (Exception ex) {}
                }
            }
            br.close();
            in.close();
        }
        if (totalUsed == 0L) totalUsed = 1L; // avoid zero
        System.out.println("TOTAL_USED = " + totalUsed);

        // JobB: filter coupons with use_count > totalUsed * 1% and with intervalCount>0; sort by avg interval
        Configuration confB = new Configuration();
        confB.set("total.used", Long.toString(totalUsed));   // 传递全局参数
        Job jobB = Job.getInstance(confB, "Task3 JobB FilterAndSortByAvgInterval");
        jobB.setJarByClass(Task3CouponAnalysis.class);
        jobB.setMapperClass(JobBMapper.class);
        jobB.setReducerClass(JobBReducer.class);
        jobB.setMapOutputKeyClass(DoubleWritable.class);
        jobB.setMapOutputValueClass(Text.class);
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(Text.class);
        jobB.setNumReduceTasks(1); // 关键：单个Reducer确保全局排序

        FileInputFormat.addInputPath(jobB, new Path(jobAOut));
        FileOutputFormat.setOutputPath(jobB, new Path(jobBOut));
        boolean okB = jobB.waitForCompletion(true);
        System.exit(okB ? 0 : 1);
    }
    
}
