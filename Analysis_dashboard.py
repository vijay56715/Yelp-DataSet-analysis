from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,StringType,IntegerType
from pyspark.sql.functions import explode,count,split,from_json,lit,struct,create_map,col,when,avg
import matplotlib
import matplotlib.pyplot as plt
import plotly
import plotly.express as px
import os
from flask import Flask, render_template
import json
from markupsafe import Markup

app = Flask(__name__,template_folder='templates')


@app.route('/')
def hello():
    spark=SparkSession.builder.enableHiveSupport().getOrCreate()


    spark.sql("use Yelp_analysis")

    df=spark.sql("select * from business_analysis")

    df1=df.select("name","stars").groupBy("name").agg(avg("stars")).orderBy(avg("stars").desc()).limit(10)
    df2=df.select("name","stars").groupBy("stars").agg(count("name"))
    df3=df.select("name","stars").groupBy("name").agg(avg("stars")).orderBy(avg("stars")).limit(10)
    df4=df.select("name","stars").groupBy("name").agg(count("stars"))
    df5=df.select("name","text","stars").groupBy("name","text").agg(avg("stars")).filter(avg("stars")<2.5)
    df6=df.select("name","text","stars").groupBy("name","text").agg(avg("stars")).filter(avg("stars")>4.5)
    #df3=df.select("date","stars").orderBy("date")
    #df2.show()

    pdf1=df1.toPandas()
    pdf2=df2.toPandas()
    pdf3=df3.toPandas()
    pdf4=df4.toPandas()
    pdf5=df5.toPandas()
    pdf6=df6.toPandas()

    fig_top = px.bar(pdf1, x='name', y='avg(stars)', title='Top Business',range_y=(0,5))
    fig_top_json = json.dumps(fig_top, cls=plotly.utils.PlotlyJSONEncoder)
    top_desc="10 Business with highest rating"
    fig_bottom = px.bar(pdf3, x='name', y='avg(stars)', title='Least Business',range_y=(0,5))
    fig_bottom_json = json.dumps(fig_bottom, cls=plotly.utils.PlotlyJSONEncoder)
    bottom_desc="10 Business with least rating"
    fig_avg_stars = px.bar(pdf2, x='stars', y='count(name)', title='Count of business')
    fig_avg_stars_json = json.dumps(fig_avg_stars, cls=plotly.utils.PlotlyJSONEncoder)
    avg_stars_desc="Number of stars under each rating"
    review_text_least_json = pdf5.to_html()
    review_text_least_desc="Review text for business with rating below 2.5"
    review_text_grt_json = pdf6.to_html()
    review_text_grt_desc="Review text for business with rating above 4.5"

    # pdf1.plot(kind='bar',y='avg(stars)',x='name',ylim=(0,5))

    # plt.show()

    # pdf3.plot(kind='bar',y='avg(stars)',x='name',ylim=(0,5))

    # plt.show()

    # pdf2.plot(kind='bar',y='count(name)',x='stars')

    # plt.show()

    # pdf4.plot(kind='bar',y='count(stars)',x='name')

    # plt.show()

    return render_template('index.html',fig_top_json=fig_top_json,top_desc=top_desc,fig_bottom_json=fig_bottom_json,bottom_desc=bottom_desc,fig_avg_stars_json=fig_avg_stars_json,avg_stars_desc=avg_stars_desc,review_text_least_json=Markup(review_text_least_json),review_text_least_desc=review_text_least_desc,review_text_grt_json=Markup(review_text_grt_json),review_text_grt_desc=review_text_grt_desc)


if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    print ("Starting app on port %d" %(port))
    app.run(debug=True, port=port, host='0.0.0.0')





# pdf3.plot(kind='line',y='stars',x='date',subplots='name')

# plt.show()