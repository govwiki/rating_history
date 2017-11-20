FROM nginx:stable
RUN apt update
RUN apt install -y python3 python3-pip
RUN pip3 install flask gunicorn
ADD app.py /var/www/app.py
ADD templates /var/www/templates/
ADD start.sh /var/www/start.sh
ADD nginx-default.conf /etc/nginx/conf.d/default.conf
WORKDIR /var/www/
CMD bash start.sh