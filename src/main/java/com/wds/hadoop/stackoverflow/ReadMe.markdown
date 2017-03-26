## 概要模式

### 数值概要

* 单词记数 CommentWordCountMRJobRunner
* 平均值 AverageMRJobRunner
* 最大最小值 MinMaxCountMRJobRunner
* 中位数、标准差 MedianStdDevMRJobRunner


### 倒序索引

* 维基百科倒序索引 WikipediaMRJobRunner

### 计数器计数

* 计算每个州的用户数 CountNumUsersByStateJobRunner

## 过滤模式

### 过滤

* Grep和简单随机抽样 GrepFilterMRJobRunner

### 布隆过滤


### Top 10

* Top10 TopTenMRJobRunner

### 去重

* UserId去重 DistinctUserMRJobRunner


## 数据组织模式

### 分层结构模式（structured to hierarchical）

* StackOverflow帖子和评论的构建 PostCommentHierachyMRJobRunner

### 分区（partitioning）和分箱（binning）模式

* 按最后访问日期对用户分区 LastAccessDatePartitionerMRJobRunner
* 分箱，按与Hadoop相关的标签分箱 BinningMRJobRunner

### 全排序(total order sorting）和混排（shuffling）

* 全排序 TotalOrderMRJobRunner
* 混排 AnonymizeMRJobRunner

### 数据生成模式（generating data pattern）

## 连接模式

### Reduce端连接

* Reduce端连接示例 ReduceSideJoinMRJobRunner

### 复制连接

* 复制连接示例 ReplicatedJoinMRJobRunner

### 组合连接

* 组合用户评论连接 CompositeUserCommentMRJobRunner

### 笛卡儿集

* 评论对比 CartesianMRJobRunner

## 元模式

### 作业链
 
 * 基本作业链 BasicJobChainMRJobRunner
 * 并行作业链 ParallelJobChainMRJobRunner
 
 ### 折叠链
 
 * 链折叠整合 ChainFoldedMRJobRunner

