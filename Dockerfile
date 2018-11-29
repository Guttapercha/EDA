FROM python:3
RUN pip3 install jupyter
RUN pip3 install pandas
RUN pip3 install numpy
RUN pip3 install matplotlib.pyplot
RUN pip3 install seaborn

   from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    from pyspark.sql.types import StructType, StructField
    from pyspark.sql.types import DoubleType, IntegerType, StringType
    import pyspark.sql.functions as func
EXPOSE 80 8888 5000

ADD EDA.py /eda
ADD MontrealAirB&B.csv /eda
WORKDIR /eda
CMD ["bash"]
