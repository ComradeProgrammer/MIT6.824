# 一门公开课MIT6.824的大作业
本仓库是对2021年春季学期MIT6.824分布式系统的公开课的大作业的实现<br>
课程网站 https://pdos.csail.mit.edu/6.824/index.html <br>
仓库中有课程全部lab 的实现，具体内容如下：
(lab4b有一点小小小小的BUG，大概是goroutine泄露，不过感觉问题应该不算太大)
- LAB1 一个简易的MapReduce的实现 <br>
lab1具体要求： https://pdos.csail.mit.edu/6.824/labs/lab-mr.html
- LAB2A 简易RAFT协议的实现（Leader 选举部分）
- LAB2B 简易RAFT协议的实现（Log commit）
- LAB2C 简易RAFT协议的实现（持久化）
- LAB2D 简易RAFT协议的实现（快照与LOG压缩）<br>
lab2具体要求：https://pdos.csail.mit.edu/6.824/labs/lab-raft.html
- LAB3A 用LAB2中实现的RAFT协议实现一个容错的分布式键值对服务（键值对服务部分）
- LAB3B 用LAB2中实现的RAFT协议实现一个容错的分布式键值对服务（快照部分）<br>
lab3具体要求 https://pdos.csail.mit.edu/6.824/labs/lab-kvraft.html
- LAB4A 用LAB2中实现的RAFT协议实现一个<strong>分片</strong>的容错的分布式键值对服务 (分片控制中心部分)
- LAB4B 用LAB2中实现的RAFT协议实现一个<strong>分片</strong>的容错的分布式键值对服务 (各个分片的键值对服务部分)<br>
lab4具体要求 https://pdos.csail.mit.edu/6.824/labs/lab-shard.html<br><br>
(上述链接可能会过期，如果过期可在公开课课程首页的历史网站部分获得新的链接)<br>
(如果不能翻墙，往年的课程视频可以从B站中获得， 链接 https://www.bilibili.com/video/BV1R7411t71W )

依照课程要求，仓库中全部lab由Golang语言实现

这门课的理论部分的笔记会陆续更新到知乎上（欢迎来看）引流.jpg
https://www.zhihu.com/people/zhuan-zhi-ge-chong-bu-fu-89

但是强烈不推荐还没开始写代码的人先阅览本仓库内容。自己从头到尾完全依靠自己的力量完成这一切是会让自己收获最大的·
