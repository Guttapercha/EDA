FROM python:3
RUN pip3 install jupyter
RUN pip3 install pandas
RUN pip3 install numpy
EXPOSE 80 8888 5000
ADD EDA.ipynb /eda/
ADD MontrealAirB&B.csv /eda/
WORKDIR /eda
CMD [ "python", "/eda/EDA.ipynb" ]
