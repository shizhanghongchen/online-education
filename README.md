# online-education
online-education

# `离线指标`
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