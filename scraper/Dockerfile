FROM python:3.8.10

ENV PYTHONUNBUFFERED 1
USER root
RUN pip install --upgrade pip
#RUN apt-get update && \
#    apt-get install -y --no-install-recommends gcc
#
#RUN python -m venv /opt/venv
#ENV PATH="/opt/venv/bin:$PATH"

# install google chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
RUN apt-get -y update
RUN apt-get install -y google-chrome-stable

# install chromedriver
RUN apt-get install -yqq unzip
RUN wget -O /tmp/chromedriver.zip http://chromedriver.storage.googleapis.com/`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE`/chromedriver_linux64.zip
RUN unzip /tmp/chromedriver.zip chromedriver -d /usr/local/bin/

ENV DISPLAY=:99

RUN mkdir /scraper
WORKDIR /scraper

COPY . /scraper/

RUN pip install -r requirements.txt

CMD unlink /tmp/download/JCR-2022-04-30.zip