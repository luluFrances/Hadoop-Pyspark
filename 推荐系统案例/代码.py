################## python 可视化代码 #################
#####文件位置
'''genome-scores.csv  genome-tags.csv  links.csv  movies.csv  ratings.csv  README.txt  tags.csv
/home/lifeng/cufe/wufanlu/spark/data/ml-20m'''
#############

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
plt.switch_backend('agg')
import re
import seaborn as sns
import time


f=open('/home/lifeng/cufe/wufanlu/spark/data/ml-20m/ratings.csv',encoding='UTF-8')
movies=pd.read_csv(f)   # movies=pd.read_csv('/home/lifeng/cufe/wufanlu/spark/data/ml-20m/movie_pd.csv')
rates=pd.read_csv(f)  # rates=pd.read_csv('/home/lifeng/cufe/wufanlu/spark/data/ml-20m/rates_pd.csv')
links=pd.read_csv(f)
tags=pd.read_csv(f)
genome_scores=pd.read_csv(f)
genome_tags=pd.read_csv(f)


def convert_date(x):
    time_local = time.localtime(x)
    dt = time.strftime("%Y-%m-%d %H:%M:%S",time_local)
    return int(dt[0:4])
rates['year']=rates['timestamp'].apply(convert_date)

avg_rates_year=rates[['year','rating']].groupby('year').mean()

def year(title):
    year=re.search(r'\(\d{4}\)',title)
    if year：
        year=year.group()
        return int(year[1:5])
    else:
        return 0

movie['year']=movie['title'].apply(year)

#####第一幅图：电影数量和电影平均平均随时间的变化趋势
sub=movies[movies['year']!=0]
plt.figure(1,figsize=(20,12))
plt.subplot(211)
plt.grid(True)
plt.plot(sub.groupby(['year']).count()['title'])
plt.title('Number of movies vs Years')

plt.subplot(212)
plt.plot(avg_rates_year)
plt.title('Average ratings vs genres')
plt.grid(True)
plt.tight_layout(pad=0.4, w_pad=0.5, h_pad=1.0)# 自动调整子图（subplot）参数来适应画板（figure）的区
plt.savefig('/home/lifeng/cufe/wufanlu/spark/data/ml-20m/movieratingVSyears.jpg')

### 第二幅图：不同类别的电影的评分 ###########
movies=pd.read_csv('/home/lifeng/cufe/wufanlu/spark/data/ml-20m/movies.csv')
rates=pd.read_csv('/home/lifeng/cufe/wufanlu/spark/data/ml-20m/ratings.csv')

def genres_str(x):
        if x == '(no genres listed)':
            keys=['no_genres']
        else:
            keys=x.split('|')
        return keys


movies['genres_split']=movies['genres'].apply(genres_str)
movierating=pd.merge(movies,rates,on='movieId')
mrsub=movierating[['genres_split','rating']]


val=[]
for i in range(mrsub.shape[0]):
    gl=mrsub.loc[i,'genres_split']
    for genres in gl:
        val.append([mrsub['rating'][i],genres])


df=pd.DataFrame(val,columns=['rating','genres'])

box=sns.boxplot(x="genres",y="rating",data=df,height=8.27, aspect=11.7/8.27)
box.set_xticklabels(box.get_xticklabels(),rotation=90)
box.figure.savefig("/home/lifeng/cufe/wufanlu/spark/data/ml-20m/boxplot.png")
############################################

########第三幅图：柱形图-不同类别电影热度
df2.sort_values("rating",inplace=True) # 按照评论数量排序
genres=df2.index.values
rating=df2.values.flatten()
df2=pd.DataFrame({'genres':genres,'rating':rating}) # 生成统计不同类别电影评论数量的数据框
# 画柱形图
fig, ax = plt.subplots(figsize=(20,6))
barplot = sns.barplot(x="genres", y="rating number", data=df2, ax=ax)
plt.savefig('/home/lifeng/cufe/wufanlu/spark/data/ml-20m/barplot3.png')
df2.to_csv("/home/lifeng/cufe/wufanlu/spark/data/ml-20m/df2.csv",index=False,sep=',')
##############################################

#######第四幅图: 不同类别电影的数量
gen=movies['genres_split']
valg=[]
for i in range(gen.shape[0]):
    for g in gen[i]:
        valg.append(g)

genres=result.index.values
count=result.values
df1=pd.DataFrame({'genres':genres,'number of movies':count})
fig, ax = plt.subplots(figsize=(20,6))
barplot = sns.barplot(x="genres", y="number of movies", data=df1, ax=ax)
plt.savefig('/home/lifeng/cufe/wufanlu/spark/data/ml-20m/barplot2.png')
#################################################


############生成csv的模板#########################
movies.to_csv("/home/lifeng/cufe/wufanlu/spark/data/ml-20m/movie_pd.csv",index=False,sep=',')
rates.to_csv("/home/lifeng/cufe/wufanlu/spark/data/ml-20m/rates_pd.csv",index=False,sep=',')
file.to_csv("/home/lifeng/cufe/wufanlu/spark/data/ml-20m/file.csv",index=False,sep=',')
################### pyspark推荐系统代码 ##################
##########初始化########
from pyspark import SparkContext,SparkConf
conf = SparkConf().setAppName("random").setMaster("local")
sc = SparkContext.getOrCreate(conf)
#########################
from pyspark.mllib.recommendation import ALS,MatrixFactorizationModel,Rating

##########数据预处理##########
def parseMovie(line):
    fields = line.strip().split(',')
    return int(fields[0]),fields[1]


def parseRating(line):
    fields = line.strip().split(',')
    return int(fields[0]),int(fields[1]),float(fields[2])

#### 提取movie中有用的部分并格式化
movie = sc.textFile('cufe/wufanlu/movies.csv')
header1=movie.first()
movie = movie.filter(lambda x: x !=header1)
movieRDD=movie.map(parseMovie)
#### 提取rating中有用的部分并格式化
rating = sc.textFile('cufe/wufanlu/ratings.csv')
header2=rating.first()
rating = rating.filter(lambda x: x !=header2)
ratingRDD=rating.map(parseRating)
#### 展示电影数量和用户数量
numRatings = ratingRDD.count()
numUsers = ratingRDD.map(lambda r:r[0]).distinct().count()
numMovies = ratingRDD.map(lambda r:r[1]).distinct().count()
print ('Ratings dataset has %d ratings from %d users on %d movies.' %(numRatings,numUsers,numMovies))

### 
RDD1,RDD2=ratingRDD.randomSplit([0.8,0.2])
trainingRDD=RDD1.cache()
testRDD =RDD2.cache()
trainingRDD.count()
testRDD.count()
###
rank=10
numIterations=10
model=ALS.train(trainingRDD,rank,numIterations)
model.save(sc, "cufe/wufanlu") # 加载模型：model = MatrixFactorizationModel.load(sc, "cufe/wufanlu")
user118205Recs=model.recommendProducts(118205,5)
###
testUserMovieRDD=testRDD.map(lambda x: (x[0],x[1]))
testUserMovieRDD.take(2)

predictionsTestRDD=model.predictAll(testUserMovieRDD).map(lambda r:((r[0],r[1]),r[2]))
predictionsTestRDD.take(2)

ratingsPredictions=testRDD.map(lambda r:((r[0],r[1]),r[2])).join(predictionsTestRDD)
ratingsPredictions.take(5)

badPredictions=ratingsPredictions.filter(lambda r: (r[1][0]<=1 and r[1][1]>=4))
badPredictions.take(2)
badPredictions.count()

MSE=ratingsPredictions.map(lambda r: (r[1][0]-r[1][1])**2).mean()

print ('Mean Squared Error ='+str(MSE))




##########备用装备############
#1 RDD API
movie = sc.textFile('cufe/wufanlu/movies.csv')
header1=movie.first()
movie = movie.filter(lambda x: x !=header1)
movieRDD=movie.map(parseMovie)
rating = sc.textFile('cufe/wufanlu/ratings.csv')
header2=rating.first()
rating = rating.filter(lambda x: x !=header2)
ratingRDD=rating.map(parseRating)
# 2 DataFrame API
movie=spark.read.option("header","true").csv('cufe/wufanlu/movies.csv')
rating=spark.read.option("header","true").csv('cufe/wufanlu/ratings.csv')
movieRDD=movie.rdd
ratingRDD=rating.rdd
# 报错时用此进入
pyspark2 --master yarn  --conf "spark.pyspark.python=/usr/bin/python3"  --conf "spark.pyspark.driver.python=/usr/bin/python3"
##################################

######测试用品##############
rdd=sc.parallelize([1,2,3,4])
rdd.count()
#########################
