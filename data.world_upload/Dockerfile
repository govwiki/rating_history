FROM debian:stable

RUN apt update && apt install -y python3 python3-pip

COPY requirements.txt /uploader/requirements.txt
RUN pip3 install -r /uploader/requirements.txt
COPY upload.py /uploader/upload.py

WORKDIR /uploader/
ENV LC_ALL=en_US.UTF-8
CMD python3 upload.py