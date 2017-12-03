FROM debian:stable

RUN apt update && apt install -y xvfb unzip libgconf-2-4 chromium python3 python3-pip

ADD https://chromedriver.storage.googleapis.com/2.33/chromedriver_linux64.zip /tmp/
RUN unzip /tmp/chromedriver_linux64.zip -d /usr/local/bin/
COPY requirements.txt /rating_history/requirements.txt
RUN pip3 install -r /rating_history/requirements.txt
COPY get_rating_history.py /rating_history/get_rating_history.py
COPY conf.ini /rating_history/conf.ini

WORKDIR /rating_history/
RUN apt install -y locales && rm -rf /var/lib/apt/lists/* \
    && localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
CMD python3 get_rating_history.py