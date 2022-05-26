# -*- coding: utf-8 -*-
 
 

from pyspark.sql import SparkSession

from pyspark.sql.functions import col, round
from pyspark.sql.functions import udf
from pyspark.sql.functions import split
from pyspark.sql.functions import row_number
from pyspark.sql.types import *
from pyspark.sql import Window
from pyspark.sql.functions import explode
from pyspark.sql.functions import explode_outer
spark = SparkSession \
    .builder \
    .appName("COMP5349 A2 Data Loading Example") \
    .getOrCreate()

 
test_data = "s3://comp5349-2022/test.json"
test_init_df = spark.read.json(test_data).select(explode('data').alias('data'))
test_data_df= test_init_df.select(col('data.title').alias('title'),(explode("data.paragraphs").alias('paragraphs')))
test_data_df.printSchema()
 

context_df= test_data_df.select('title',col("paragraphs.context").alias('context'),col("paragraphs.qas").alias('qas'))
context_df.printSchema()

def seperate_word(context):
    start_end = []
    start = 0
    end = 4096
 
    while start < len(context):
      info = (context[start:end],start,end)
      start_end.append(info)
   
      start += 2048
      end += 2048
      if end > len(context):
        end = len(context)
    return start_end

schema = StructType([ \
    StructField("source",StringType(),True), \
    StructField("start",IntegerType(),True), \
    StructField("end",IntegerType(),True)
  ])

convertUDF = udf(seperate_word,ArrayType(schema))
context = context_df.select('title',convertUDF(col('context')).alias('context'),'qas') 
context_expand_df = context.select('title',explode('context').alias('context'),'qas')
qas_df = context_expand_df.select('title','context',explode('qas').alias('qas'))

qas_expand = qas_df.select('title','context.*',col('qas.id').alias('qas_id'),'qas.question','qas.is_impossible',explode_outer('qas.answers').alias('answers'))
qas_expand.printSchema()

answer = qas_expand.select('title','source','start','end','qas_id','question','is_impossible','answers.*')

def negative_positive(start, end, is_impossible, answer_start, text):
  if is_impossible == True:
    return ('impossible_negative',0,0)
  else:
    answer_end = answer_start+len(text)
    if start <= answer_start and answer_end <= end:
      return ('positive',answer_start-start, answer_end-start)
    elif start<= answer_start and answer_end > end and answer_start < end:
      return ('positive', answer_start-start , end-start)
    elif answer_start < start and answer_end <= end and start < answer_end:
      return ('positive', 0 , answer_end-start)
    else:
      return ('possible_negative',0,0)

answerSchema = StructType([ \
    StructField("is_impossible",StringType(),True), \
    StructField("answer_start",IntegerType(),True), \
    StructField("answer_end",IntegerType(),True)
  ])

answer_with_positionUDF = udf(negative_positive,answerSchema)
qas_impossible_answer = answer.select('title','source','start','end','qas_id','question',\
                        answer_with_positionUDF(col('start'),col('end'),col('is_impossible'),col('answer_start'),col('text')).alias('answer_state'),\
                        'text')
qas_impossible_answer.printSchema()

expand_df = qas_impossible_answer.select('title','source','start','end','qas_id','question','answer_state.*','text')
 
expand_df.count()

## filter impossible question in a contract
negative_question = expand_df.select('title','qas_id','question','is_impossible','answer_start','answer_end','source')\
                              .filter(col('is_impossible')=='impossible_negative')
 
negative_question.count()

positive = expand_df.select('title','source','question','qas_id','answer_start','answer_end','is_impossible')\
                              .filter(col('is_impossible')=='positive')
 
positive_question = positive.groupby('title','question')\
                            .count()\
                 
positive_question.show()

positive_avg = positive_question.join(positive,'title' and 'question','left')\
                  .groupby('question')\
                  .avg('count')\
                  .select('question',round('avg(count)').alias('average')) 
                
positive_avg.count()

window = Window.partitionBy("question").orderBy("question")

negative = negative_question\
                 .join(positive, "title" and "source","leftanti")\
                 .join(positive_avg , 'question','left')\
                 .withColumn("question_order", row_number().over(window))\
                 .select('*')\
                 .where(col('question_order') <= col('average'))\

negative.show()

negative.count()

possible_negative_question = expand_df.select('qas_id','question','is_impossible','answer_start','answer_end','source')\
                              .filter(col('is_impossible')=='possible_negative')
 
possible_negative_question.count()

positive_all= positive.groupby('qas_id')\
                       .count()
positive_all.count()

window = Window.partitionBy("qas_id").orderBy("qas_id")
possible_negative = possible_negative_question.join(positive_all , 'qas_id','left')\
                 .join(positive,'title' and "source","leftanti")\
                 .withColumn("question_order", row_number().over(window))\
                 .select('*')\
                 .where(col('question_order') <= col('count'))\

possible_negative.show()

possible_negative.count()

possible_negative.printSchema()

positive.printSchema()

negative.printSchema()

final_df = negative.select('source','question','answer_start','answer_end')\
.union(positive.select('source','question','answer_start','answer_end')\
       .union(possible_negative.select('source','question','answer_start','answer_end')))

final_df.show()

final_df.count()

final_df.coalesce(1).write.mode('Overwrite').format('json').save('result.json')


spark.stop()
