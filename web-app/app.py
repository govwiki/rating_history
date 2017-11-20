import os
from werkzeug.contrib.fixers import ProxyFix
from flask import Flask, render_template


app = Flask(__name__)


@app.route('/')
def files_list():
    csv_path = os.environ.get('CSV_PATH', '/var/csv_path/')
    if not os.path.exists(csv_path):
        error_msg = 'ERROR: csv_path dir doesn\'t exist!'
    elif not os.path.isdir(csv_path):
        error_msg = 'ERROR: csv_path parameter points to file!'
    else:
        error_msg = ''
    files = sorted(os.listdir(csv_path))
    return render_template(
        'files_list.html',
        error_msg=error_msg,
        files=files,
    )


app.wsgi_app = ProxyFix(app.wsgi_app)
if __name__ == '__main__':
    app.run()
