package com.example;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Task4MerchantAggregate
 *
 * 输入: ccf_offline_stage1_train.csv (列顺序假定为:
 *   User_id,Merchant_id,Coupon_id,Discount_rate,Distance,Date_received,Date)
 *
 * 输出: 每个 merchant 一行:
 *   <Merchant_id> \t <avg_discount_or_NA> \t <avg_distance_or_NA> \t <used_count>
 *
 * 规则:
 * - 只解析 Discount_rate 含 ":" 的满减形式 (x:y[:...])，取 x 和 y 的前两段计算 discount = y/x。
 * - 对同一 merchant 去重 discount 值后取算术平均（即不同折扣的平均）。
 * - 平均距离对每个距离值以 user 去重（unique users per distance），然后按 weighted average 计算。
 * - used_count: Coupon_id != null 且 Date != null 的记录计数。
 */

public class Task4MerchantAggregate {

    public static class Mapper4 extends Mapper<LongWritable, Text, Text, Text> {
        // 输入：<行偏移值(LongWritable)，一行的文本(Text)>
        // 输出：<商家ID(Merchant_id)，DISC/DIST/USE>
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString();
            if (line == null) return;   
            line = line.trim();
            if (line.isEmpty()) return;   // 跳过空行
            String low = line.toLowerCase();
            if (low.startsWith("user_id") || low.contains("user_id,")) return; // 跳过表头

            String[] f = line.split(",", -1);
            if (f.length < 7) return;

            String userId = f[0].trim();
            String merchant = f[1].trim();
            String coupon = f[2].trim();
            String discountRaw = f[3].trim();
            String distanceStr = f[4].trim();
            String date = f[6].trim();

            if (merchant.isEmpty()) return;

            outKey.set(merchant);

            // Discount: 只处理"满减"格式的折扣（如"100:20"）
            if (discountRaw != null && discountRaw.length() > 0 && !discountRaw.equalsIgnoreCase("null") && discountRaw.contains(":")) {
                String[] parts = discountRaw.split(":");
                if (parts.length >= 2) {
                    try {
                        String p0 = parts[0].replaceFirst("^0+(?!$)", "");
                        String p1 = parts[1].replaceFirst("^0+(?!$)", "");
                        int denom = Integer.parseInt(p0);
                        int numer = Integer.parseInt(p1);
                        if (denom > 0) {
                            double disc = ((double) numer) / ((double) denom);
                            // 计算折扣率 = y/x (如 20/100 = 0.2)
                            String discStr = String.format("%.6f", disc);
                            outVal.set("DISC\t" + discStr);
                            // 输出格式: "DISC\t折扣值"
                            ctx.write(outKey, outVal);
                        }
                    } catch (NumberFormatException ex) {
                        // skip malformed discount
                    }
                }
            }

            // Distance contribution: only if distance present and numeric
            if (distanceStr != null && distanceStr.length() > 0 && !distanceStr.equalsIgnoreCase("null")) {
                if (distanceStr.matches("-?\\d+")) {
                    //// 输出格式: "DIST\t距离\t用户ID"
                    outVal.set("DIST\t" + distanceStr + "\t" + userId);
                    ctx.write(outKey, outVal);
                }
            }

            // used_count marker
            if (coupon != null && coupon.length() > 0 && !coupon.equalsIgnoreCase("null")
                    && date != null && date.length() > 0 && !date.equalsIgnoreCase("null")) {
                outVal.set("USE\t1");
                // 输出格式: "DIST\t距离\t用户ID"
                ctx.write(outKey, outVal);
            }
        }
    }

    public static class Reducer4 extends Reducer<Text, Text, Text, Text> {
        private final int MAX_D = 10; // tolerate distances up to 100
        @Override
        protected void reduce(Text key, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException {
            
            HashSet<String> discountSet = new HashSet<>();  // 存储去重后的折扣值

            @SuppressWarnings("unchecked")
            HashSet<String>[] userSets = new HashSet[MAX_D + 1];   // 按距离值存储用户集合（用于去重）
            for (int i = 0; i <= MAX_D; i++) userSets[i] = null;

            long usedCount = 0L;   // 使用次数计数器

            for (Text t : vals) {
                String s = t.toString();
                if (s == null || s.length() == 0) continue;
                String[] p = s.split("\t", -1);
                if (p.length == 0) continue;
                String tag = p[0];
                if (tag.equals("DISC")) {  // 折扣率
                    if (p.length >= 2) {
                        discountSet.add(p[1]);
                    }
                } else if (tag.equals("DIST")) {  // 距离
                    if (p.length >= 3) {
                        String dstr = p[1];
                        String uid = p[2];
                        try {
                            int d = Integer.parseInt(dstr);
                            if (d < 0 || d > MAX_D) continue;
                            if (userSets[d] == null) userSets[d] = new HashSet<String>();
                            userSets[d].add(uid);
                        } catch (NumberFormatException ex) {
                            // skip
                        }
                    }
                } else if (tag.equals("USE")) {  // 使用次数
                    if (p.length >= 2) {
                        try {
                            usedCount += Long.parseLong(p[1]);
                        } catch (NumberFormatException ex) {
                            // ignore
                        }
                    }
                }
            }

            // compute avg discount over distinct discounts
            String avgDiscountStr = "NA";
            if (!discountSet.isEmpty()) {
                double sum = 0.0;
                int cnt = 0;
                for (String ds : discountSet) {
                    try {
                        double v = Double.parseDouble(ds);
                        sum += v;
                        cnt++;
                    } catch (NumberFormatException ex) { }
                }
                if (cnt > 0) {
                    double avg = sum / cnt;
                    avgDiscountStr = String.format("%.6f", avg);
                }
            }

            // compute avg distance: weighted by unique users per distance
            long totalUsers = 0L;
            long weightedSum = 0L;
            for (int d = 0; d <= MAX_D; d++) {
                HashSet<String> set = userSets[d];
                if (set != null && !set.isEmpty()) {
                    long c = set.size();
                    totalUsers += c;
                    weightedSum += c * (long)d;
                }
            }
            String avgDistStr = "NA";
            if (totalUsers > 0) {
                double avgDist = ((double) weightedSum) / ((double) totalUsers);
                avgDistStr = String.format("%.6f", avgDist);
            }

            // output: merchant \t avgDiscount(or NA) \t avgDist(or NA) \t usedCount
            Text outKey = new Text(key.toString());
            Text outVal = new Text(avgDiscountStr + "\t" + avgDistStr + "\t" + usedCount);
            ctx.write(outKey, outVal);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Task4MerchantAggregate <input> <output>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task4 Merchant Aggregate (avg discount, avg distance, used count)");
        job.setJarByClass(Task4MerchantAggregate.class);
        job.setMapperClass(Mapper4.class);
        job.setReducerClass(Reducer4.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
