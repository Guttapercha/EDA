try:
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    import seaborn as sns
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    from pyspark.sql.types import StructType, StructField
    from pyspark.sql.types import DoubleType, IntegerType, StringType
    import pyspark.sql.functions as func
except Exception as e:
    print(e)


def exploration():
    sc = SparkContext("local", "EDA")

    spark = SparkSession.builder.appName('EDA').getOrCreate()

    schema = StructType([
    StructField("neighbourhood", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("room_type", StringType()),
    StructField("price", IntegerType()),
    StructField("minimum_nights", IntegerType()),
    StructField("availability_365", IntegerType())
    ])

    df = spark.read.csv('MontrealAirB&B_cleaned.csv', header=True, schema=schema)
    df.show(5)

    print(type(df))
    df.count()
    df.columns

    df_count = df.groupBy("neighbourhood").count()
    df_count.show()

    avg_price = df.groupBy("neighbourhood").agg({"price" : 'mean'})
    # round the price and rename the column
    df_avg_price = avg_price.withColumn("avg(price)", func.round(avg_price["avg(price)"], 2)).withColumnRenamed("avg(price)","average_price")
    df_avg_price.show()

    avg_min_stay = df.groupBy("neighbourhood").agg({"minimum_nights" : 'mean'})
    # round the nights and rename the column
    df_avg_min_stay = avg_min_stay.withColumn("avg(minimum_nights)", func.round(avg_min_stay["avg(minimum_nights)"], 1)).withColumnRenamed("avg(minimum_nights)","avg_min_stay")
    df_avg_min_stay.show()

    # count properties and crosstab
    df_cross = df.crosstab('neighbourhood', 'room_type').withColumnRenamed("Entire home/apt","Home (#)").withColumnRenamed("Private room","Private (#)").withColumnRenamed("Shared room","Shared (#)")
    df_cross = df_cross.withColumnRenamed("neighbourhood_room_type","neighbourhood")
    df_cross.show()

    # avg price per type of room in each district - using table pivoting
    avg_price = df.groupBy("neighbourhood").pivot('room_type').avg("price")
    # round the price and rename the column
    df_avg_price2 = avg_price.withColumn("Entire home/apt", func.round(avg_price["Entire home/apt"], 2)).withColumn("Private room", func.round(avg_price["Private room"], 2)).withColumn("Shared room", func.round(avg_price["Shared room"], 2))
    df_avg_price2 = df_avg_price2.withColumnRenamed("Entire home/apt","Entire ($)").withColumnRenamed("Private room","Private ($)").withColumnRenamed("Shared room","Shared ($)")


    # replace null values by zeros
    df_avg_price2 = df_avg_price2.na.fill(0)
    df_avg_price2.show()

    #merging all 5 created dataframes in one

    df_final = df_count.join(df_avg_price, "neighbourhood")
    df_final = df_final.join(df_avg_min_stay, "neighbourhood")
    df_final = df_final.join(df_avg_price2, "neighbourhood")
    df_final = df_final.join(df_cross, "neighbourhood")
    df_final.show()

    # filter out neighbours having less than 10 offers on AirBnB
    df_final.filter(df_final['count'] > 10).show()

    #Save data to file
    df_final.toPandas().to_csv('MontrealAirB&B_cleaned_processed.csv')

    #DESCRIPTIVE ANALYTICS
    plt.figure(figsize=(16, 14))

    # convert to Pandas Dataframe
    fd1 = df_final.toPandas()

    #take first 10 districts based on the number of offers
    fd1 = fd1.nlargest(10,"count")

    #left top plot
    plt.subplot(221)

    entire = fd1["Home (#)"]
    private = fd1["Private (#)"]
    shared = fd1["Shared (#)"]

    ind = np.arange(10)

    p1 = plt.bar(ind, entire, color='#d02728')
    p2 = plt.bar(ind, private,  bottom=entire, color='green')
    p3 = plt.bar(ind, shared, bottom=entire+private, color='black')

    plt.ylabel('count', fontsize=16)
    plt.xticks([], fontsize=16)
    plt.yticks(fontsize=16)
    plt.title("Offers vs neighbourhood (top 10)", fontsize=18)
    plt.legend((p1[0], p2[0], p3[0]), ('entire', 'private', "shared"), fontsize=16)

    # left bottom plot
    plt.subplot(223)

    entire = fd1["Entire ($)"]
    private = fd1["Private ($)"]
    shared = fd1["Shared ($)"]

    ind = np.arange(10)

    p1 = plt.bar(ind, entire, color='#d02728')
    p2 = plt.bar(ind, private,  bottom=entire, color='green')
    p3 = plt.bar(ind, shared, bottom=entire+private, color='black')

    plt.ylabel('price per night', fontsize=16)
    plt.xticks(ind, fd1["neighbourhood"], fontsize=16, rotation=90)
    plt.yticks(fontsize=16)

    plt.title("Offers vs neighbourhood (top 10). Average prices.", fontsize=18)
    plt.legend((p1[0], p2[0], p3[0]), ('entire', 'private', "shared"), fontsize=16)

    # top right plot
    plt.subplot(222)

    colors = np.random.rand(10)
    ax3 = plt.scatter(fd1["avg_min_stay"], fd1["average_price"], s = 125, c = colors, alpha=0.5)
    plt.title("Average price vs min nights stay (top 10)", fontsize=18)
    plt.ylabel('price per night', fontsize=16)
    plt.xlabel('min nights stay', fontsize=16)
    plt.xticks(fontsize=16)
    plt.yticks(fontsize=16)


    #bottom right plot
    plt.subplot(224)

    ax4=plt.bar(fd1["neighbourhood"], fd1["avg_min_stay"], align='center', color="orange")
    plt.xticks(rotation="vertical", fontsize=16)
    plt.title("Average min nights vs neighbourhood (top 10)", fontsize=18)
    plt.ylabel('average min nights stay', fontsize=16)
    plt.yticks(fontsize=16)


    plt.show()
    sc.stop()


if __name__ == "__main__":
    exploration()
