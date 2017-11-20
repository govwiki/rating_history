gunicorn app:app &
nginx -g 'daemon off;'
