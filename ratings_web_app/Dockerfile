FROM nginx:stable
RUN apt update
RUN apt install -y python3 python3-pip
RUN pip3 install Django==2.0 gunicorn
ADD manage.py /var/www/manage.py
ADD templates /var/www/templates/
ADD ratings /var/www/ratings
ADD ratings_web_app /var/www/ratings_web_app/
ADD assets /var/www/assets
ADD assets /var/www/static
ADD start.sh /var/www/start.sh
ADD nginx-default.conf /etc/nginx/conf.d/default.conf
WORKDIR /var/www/
CMD bash start.sh