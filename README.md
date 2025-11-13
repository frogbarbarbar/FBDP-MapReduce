# 实验二：MapReduce 编程实践

## 项目简介
Hadoop 实验课程的第二次实验 —— **MapReduce 编程实践**。
目标：分析“优惠券使用行为”数据，统计并探索影响优惠券使用的关键因素。

---

## 实验任务
| 任务 | 内容 | 输入文件 | 输出格式 |
|------|------|-----------|-----------|
| 1 | 统计三类样本数量 | offline + online | `<Merchant_id> <负样本数> <普通消费数> <正样本数>` |
| 2 | 商家距离分布统计 | ccf_offline_stage1_train.csv | `<Merchant_id> <距离为x的消费者人数>` |
| 3 | 优惠券使用次数与平均消费间隔 | ccf_offline_stage1_train.csv | `<Coupon_id> <平均消费间隔>` |
| 4 | 折扣率与距离影响分析 | ccf_offline_stage1_train.csv | `<Merchant_id> <平均折扣率> <平均距离> <使用次数>` |

---

## 环境配置
- Ubuntu 22.04（VMware 虚拟机）
- Hadoop 3.4.0（伪分布式）
- JDK 1.8
- Maven
- VS Code 远程开发

---

## 项目结构
- 此项目的树状结构图如下：
```
mapreduce-lab/
├── pom.xml
├── assignment.md
├── README.md
├── src/
│ └── main/java/com/example/
│     ├── Task1SampleCount.java
│     ├── Task2MerchantDistance.java
│     ├── Task3CouponUsage.java
│     └── Task4MerchantAggregate.java
├── ccf_offline_stage1_train.csv
├── ccf_online_stage1_train.csv
├── output/
│     ├── task1_offline/
│     ├── task1_online/
│     ├── task2/
│     ├── task3_jobA/
│     ├── task3_jobB/
│     └── task4/
├── target/
└──.gitignore
```
- 此项目基于maven构建，也提供了相应的pom.xml，因此可以直接用maven来管理此java项目。

## 运行方式
```bash
mvn clean package

# 运行程序前，请先清理输出文件（如果之前已经存在）
hdfs dfs -rm -r -f /user/hadoop/lab2/output

# 任务一（以线下数据文件为例）
hadoop jar target/mapreduce-lab-1.0-SNAPSHOT.jar com.example.Task1MerchantBehavior \
/user/hadoop/lab2/input/ccf_offline_stage1_train.csv \
/user/hadoop/lab2/output/task1_offline \
offline

# 任务二
hadoop jar target/mapreduce-lab-1.0-SNAPSHOT.jar com.example.Task2MerchantDistance \
/user/hadoop/lab2/input/ccf_offline_stage1_train.csv \
/user/hadoop/lab2/output/task2

# 任务三
hadoop jar target/mapreduce-lab-1.0-SNAPSHOT.jar com.example.Task3CouponAnalysis \
/user/hadoop/lab2/input/ccf_offline_stage1_train.csv \
/user/hadoop/lab2/output/task3_jobA \
/user/hadoop/lab2/output/task3_jobB

# 任务四
hadoop jar target/mapreduce-lab-1.0-SNAPSHOT.jar com.example.Task4MerchantAggregate \
/user/hadoop/lab2/input/ccf_offline_stage1_train.csv \
/user/hadoop/lab2/output/task4
```

## 实验结果
实验结果的相关输出、Web界面截图，均在实验报告中进行了展示。

## 总结
本项目是一个基于Hadoop MapReduce的电商数据分析系统，通过四个独立的MapReduce任务，从不同维度挖掘商户行为模式、用户距离分布、优惠券使用特征等关键商业洞察。通过该实验，我进一步理解了 MapReduce 的核心思想和执行流程，并对大数据任务的性能优化与可扩展性设计有了更深认识。

作者：欧阳慧婷 231275019 
环境：Ubuntu 22.04 + Hadoop 3.4.0 + Maven 3.6.3 + Java 1.8 
提交日期：2025年11月13日
