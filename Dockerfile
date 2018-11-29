FROM python:3
RUN pip3 install jupyter
EXPOSE 80 8888 5000
ADD AirB&B /airbnb
WORKDIR /airbnb/app
RUN pip3 install -r requirements.txt
CMD ["bash"]
