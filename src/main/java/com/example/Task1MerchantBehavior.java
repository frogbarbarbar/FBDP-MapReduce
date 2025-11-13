package com.example;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task1MerchantBehavior {

    public static class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        // 输入：key 为 LongWritable（行偏移量）, value 为 Text（一行文本）
        // 输出：key 为 Text（商户ID）, value 为 IntWritable（取值为 -1 或 0 或 1）
        private final static IntWritable NEG = new IntWritable(-1); // 领取优惠券但未使用 - 负样本
        private final static IntWritable ORD = new IntWritable(0);  // 普通消费
        private final static IntWritable POS = new IntWritable(1);  // 用优惠券消费 - 正样本
        private Text outKey = new Text();

        // configuration-driven flags / indices
        private boolean isOffline = true;

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            // read the global parameter "input.type" (expect "offline" or "online")
            String inputType = conf.get("input.type", "offline").trim().toLowerCase();
            if (inputType.equals("online")) {
                isOffline = false;
            } else {
                isOffline = true;
            }
            // System.out.println("Mapper setup: input.type=" + inputType + ", idxCoupon=" + idxCoupon);
        }

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString();
 
            if (line == null || line.trim().length() == 0) return;   // 跳过空行

            String low = line.toLowerCase();
            if (low.startsWith("user_id") || low.contains("user_id,") ) return;  // 跳过表头，即第一行

            String[] f = line.split(",", -1); // 分割CSV行，保留空字段
            try {
                String merchant="", coupon="", date="";
                // offline expected: User_id,Merchant_id,Coupon_id,Discount_rate,Distance,Date_received,Date
                // online expected:  User_id,Merchant_id,Action,Coupon_id,Discount_rate,Date_received,Date
                if (f.length >= 7) {
                    merchant = f[1].trim();   // 商家标识符
                    if (merchant.length() == 0) return;

                    if (isOffline) {
                        coupon = f[2].trim();  // 线下数据：优惠券在第3列
                    } else {
                        coupon = f[3].trim();  // 线上数据：优惠券在第4列
                    }
                    
                    date = f[f.length - 1].trim();  // 优惠券使用日期
                } else {
                    return;
                }

                boolean hasCoupon = !isEmpty(coupon) && !coupon.equalsIgnoreCase("null");
                boolean hasDate = !isEmpty(date) && !date.equalsIgnoreCase("null");

                outKey.set(merchant);
                if (hasCoupon && !hasDate) {
                    ctx.write(outKey, NEG);
                } else if (!hasCoupon && hasDate) {
                    ctx.write(outKey, ORD);
                } else if (hasCoupon && hasDate) {
                    ctx.write(outKey, POS);
                } // else coupon==null and date==null -> ignore
            } catch (Exception ex) {
                // 忽略异常行，也可记录日志
            }
        }

        private boolean isEmpty(String s) {
            return s == null || s.trim().length() == 0;
        }
    }

    public static class Reducer1 extends Reducer<Text, IntWritable, Text, Text> {
        // 输入：<商户ID, [行为标记列表]>
        // 输出：<商户ID, 统计结果>   （格式为：商户ID \t 负样本数 \t 普通消费数 \t 正样本数）
        @Override
        protected void reduce(Text key, Iterable<IntWritable> vals, Context ctx) throws IOException, InterruptedException {
            long neg=0, ord=0, pos=0;
            for (IntWritable v : vals) {
                int x = v.get();
                if (x < 0) neg++;
                else if (x == 0) ord++;
                else if (x > 0) pos++;
            }
            ctx.write(key, new Text(neg + "\t" + ord + "\t" + pos));
        }
    }

    public static void main(String[] args) throws Exception {
        // usage: hadoop jar ... Task1MerchantBehavior <input> <output> [offline|online]
        if (args.length < 2) {
            System.err.println("Usage: Task1MerchantBehavior <input> <output> [offline|online]");
            System.exit(2);
        }

        String inputType = "offline";
        if (args.length >= 3) inputType = args[2];

        Configuration conf = new Configuration();
        // put the user-specified type into Configuration so Mapper can read it
        conf.set("input.type", inputType.trim().toLowerCase());

        Job job = Job.getInstance(conf, "Task1 Merchant Behavior");
        job.setJarByClass(Task1MerchantBehavior.class);
        job.setMapperClass(Mapper1.class);
        job.setReducerClass(Reducer1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));   
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
