# BASIC
FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteracitve

# WORKDIR
WORKDIR /var/www/LAB

# initialize
RUN apt-get update && apt-get -y upgrade && apt-get -y dist-upgrade && apt-get -y autoremove && apt-get clean
RUN apt install -y software-properties-common
RUN apt-get install -y python2 python3.6 python3-pip git-all curl --fix-missing
RUN apt-get install -y libcurl4-openssl-dev libxml2 libxml2-dev libxslt1-dev build-essential libgmp-dev zlib1g-dev

# pip
WORKDIR /var/www/LAB
COPY requirements.txt /var/www/LAB
RUN pip3 --trusted-host pypi.org --trusted-host files.pythonhosted.org install -r requirements.txt
RUN pip3 install pika

# playwright
# RUN playwright install chromium
# RUN playwright install-deps

# eureka?
# RUN pip3 --trusted-host pypi.org --trusted-host files.pythonhosted.org install py_eureka_client

# kafka

# EXIT
WORKDIR /var/www/LAB
COPY . /var/www/LAB/
