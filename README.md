# online-education
online-education

# `离线数仓`
## `用户注册模块`
#### &emsp;ETL数据清洗
&emsp;&emsp;需求1 : 必须使用Spark进行数据清洗,对用户名、手机号、密码进行脱敏处理,并使用Spark将数据导入到dwd层hive表中;  
&emsp;&emsp;清洗规则 用户名 : 王XX   手机号 : 137*****789  密码直接替换成`******`
#### &emsp;基于dwd层表合成dws层的宽表
&emsp;&emsp;需求2 : 对dwd层的6张表进行合并生成一张宽表,先使用Spark Sql实现。然后需要使用DataFrame api实现功能,并对join进行优化;
#### &emsp;拉链表
&emsp;&emsp;需求3 : 针对dws层宽表的支付金额(paymoney)和vip等级(vip_level)这两个会变动的字段生成一张拉链表,需要一天进行一次更新;
#### &emsp;报表层各指标统计
&emsp;&emsp;需求4 : 使用Spark DataFrame Api统计通过各注册跳转地址(appregurl)进行注册的用户数,然后再写Spark Sql;  
&emsp;&emsp;需求5 : 使用Spark DataFrame Api统计各所属网站(sitename)的用户数,然后再写Spark Sql;  
&emsp;&emsp;需求6 : 使用Spark DataFrame Api统计各所属平台的(regsourcename)用户数,然后再写Spark Sql;  
&emsp;&emsp;需求7 : 使用Spark DataFrame Api统计通过各广告跳转(adname)的用户数,然后再写Spark Sql;  
&emsp;&emsp;需求8 : 使用Spark DataFrame Api统计各用户级别(memberlevel)的用户数,然后再写Spark Sql;  
&emsp;&emsp;需求9 : 使用Spark DataFrame Api统计各分区网站、用户级别下(dn、memberlevel)的top3用户,然后再写Spark Sql;  

## `用户做题模块`
#### &emsp;解析数据 : 
&emsp;&emsp;需求1 : 使用spark解析ods层数据,将数据存入到对应的hive表中,要求对所有score分数字段进行保留两位1位小数并且四舍五入;
#### &emsp;维度退化 : 
&emsp;&emsp;需求2 : 基于dwd层基础表数据需要对表进行维度退化进行表聚合;  
&emsp;&emsp;&emsp;&emsp;&emsp;1. dws.dws_qz_chapter ---> (章节维度表);  
&emsp;&emsp;&emsp;&emsp;&emsp;2. dws.dws_qz_course ---> (课程维度表);  
&emsp;&emsp;&emsp;&emsp;&emsp;3. dws.dws_qz_major ---> (主修维度表);  
&emsp;&emsp;&emsp;&emsp;&emsp;4. dws.dws_qz_paper ---> (试卷维度表);  
&emsp;&emsp;&emsp;&emsp;&emsp;5. dws.dws_qz_question ---> (题目维度表);  
&emsp;&emsp;&emsp;&emsp;&emsp;6. 使用spark sql和dataframe api操作;
#### &emsp;宽表合成 : 
&emsp;&emsp;需求3 : 基于dws.dws_qz_chapter、dws.dws_qz_course、dws.dws_qz_major、dws.dws_qz_paper、dws.dws_qz_question、dwd.dwd_qz_member_paper_question 合成宽表dw.user_paper_detail,使用spark sql和dataframe api操作;  
#### &emsp;报表层各指标统计 : 
&emsp;&emsp;需求1 : 基于dws.dws_user_paper_detail用户试卷详情宽表数据,统计各试卷平均耗时 平均分指标;  
&emsp;&emsp;需求2 : 基于dws.dws_user_paper_detail用户试卷详情宽表数据,统计试卷 最高分 最低分指标;  
&emsp;&emsp;需求3 : 基于dws.dws_user_paper_detail用户试卷详情宽表数据,按试卷分组获取每份试卷的分数前三用户详情;  
&emsp;&emsp;需求4 : 基于dws.dws_user_paper_detail用户试卷详情宽表数据,按试卷分组获取每份试卷的分数倒数三的用户详情;  
&emsp;&emsp;需求5 : 基于dws.dws_user_paper_detail用户试卷详情宽表数据,统计各试卷 各分段学员名称;  
&emsp;&emsp;需求6 : 基于dws.dws_user_paper_detail用户试卷详情宽表数据,统计各试卷未及格人数 及格人数 及格率;  
&emsp;&emsp;需求7 : 基于dws.dws_user_paper_detail用户试卷详情宽表数据,统计各题 正确人数 错误人数 错题率 top3错误题数多的questionid;  
#### &emsp;将数据导入mysql : 
&emsp;&emsp;需求1 : 统计指标数据导入到ads层后,通过datax将ads层数据导入到mysql中;

# `实时计算`
## `用户注册模块`
#### &emsp;实时统计注册人员信息 : 
&emsp;&emsp;需求1 : 实时统计注册人数批次为3秒一批,使用updateStateBykey算子计算历史数据和当前批次的数据总数(仅此需求使用updateStateBykey,后续需求不使用updateStateBykey);  
&emsp;&emsp;需求2 : 每6秒统统计一次1分钟内的注册数据,不需要历史数据(使用reduceByKeyAndWindow算子);  
&emsp;&emsp;需求3 : 观察对接数据,尝试进行调优;  

## `用户做题模块`
#### &emsp;实时计算学员做题算正确率与知识点掌握度 : 
&emsp;&emsp;需求1 : 要求Spark Streaming保证数据不丢失,每秒100条处理速度需要手动维护偏移量;  
&emsp;&emsp;需求2 : 同一个用户做在同一门课程同一知识点下做题需要去重,需要根据历史数据进行去重并且记录去重后的做题id与个数;  
&emsp;&emsp;需求3 : 计算知识点正确率 正确率计算公式 : 做题正确总个数 / 做题总数(保留两位小数);  
&emsp;&emsp;需求4 : 计算知识点掌握度 : 去重后的做题个数 / 当前知识点总题数(已知30题) * 当前知识点的正确率;  
#### &emsp;实时统计商品页到订单页,订单页到支付页转换率 : 
&emsp;&emsp;需求1 : 计算首页总浏览数、订单页总浏览数、支付页面总浏览数;  
&emsp;&emsp;需求2 : 计算商品课程页面到订单页的跳转转换率、订单页面到支付页面的跳转转换率;  
&emsp;&emsp;需求3 : 根据ip得出相应省份,展示出top3省份的点击数,需要根据历史数据累加;
#### &emsp;实时统计学员播放视频各时长 : 
&emsp;&emsp;需求1 : 计算各章节下的播放总时长(按chapterid聚合统计播放总时长);  
&emsp;&emsp;需求2 : 计算各课件下的播放总时长(按cwareid聚合统计播放总时长);  
&emsp;&emsp;需求3 : 计算各辅导下的播放总时长(按edutypeid聚合统计播放总时长);  
&emsp;&emsp;需求4 : 计算各播放平台下的播放总时长(按sourcetype聚合统计播放总时长);  
&emsp;&emsp;需求5 : 计算各科目下的播放总时长(按subjectid聚合统计播放总时长);  
&emsp;&emsp;需求6 : 计算用户学习视频的播放总时长、有效时长、完成时长m需求记录视频播历史区间,对于用户多次学习的播放区间不累计有效时长和完成时长;  
&emsp;&emsp;播放总时长计算 : (te-ts) / 1000 (向下取整,单位 : 秒);  
&emsp;&emsp;完成时长计算 : 根据pe-ps计算(需要对历史数据进行去重处理);  
&emsp;&emsp;有效时长计算 : 根据te-ts除以pe-ts,先计算出播放每一区间需要的实际时长 * 完成时长;