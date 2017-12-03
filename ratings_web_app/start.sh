python3 manage.py migrate
echo yes | python3 manage.py collectstatic

gunicorn ratings_web_app.wsgi &
nginx -g 'daemon off;'