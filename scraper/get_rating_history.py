from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from collections import OrderedDict
import dateparser
import xmltodict
import time
import os
import csv
import sys
import sqlite3
import logging
import shutil
import zipfile
import glob
import configparser

from webdriver_manager.chrome import ChromeDriverManager


class CSVExporter:
    column_names_map = OrderedDict((
        ('RAN', 'rating_agency_name'),
        ('FCD', 'file_creating_date'),
        ('SSC', 'sec_category'),
        ('ISSNAME', 'issuer_name'),
        ('LEI', 'legal_entity_identifier'),
        ('OBT', 'object_type_rated'),
        ('INSTNAME', 'instrument_name'),
        ('CUSIP', 'CUSIP_number'),
        ('CR', 'coupon_date'),
        ('MD', 'maturity_date'),
        ('PV', 'par_value'),
        ('IP', 'issued_paid'),
        ('R', 'rating'),
        ('RAD', 'rating_action_date'),
        ('RAC', 'rating_action_class'),
        ('RT', 'rating_type'),
        ('RST', 'rating_sub_type'),
        ('RTT', 'rating_type_term'),
        ('OAN', 'other_announcement_type'),
        ('WST', 'watch_status'),
        ('ROL', 'rating_outlook'),
        ('ISI', 'issuer_identifier'),
        ('ISIS', 'issuer_identifier_schema'),
        ('INI', 'instrument_identifier'),
        ('INIS', 'instrument_identifier_schema'),
        ('CIK', 'central_index_key'),
        ('OI', 'obligor_identifier'),
        ('OIS', 'obligor_identifier_schema'),
        ('OIOS', 'obligor_identifier_other'),
        ('OSC', 'obligor_sec_category',),
        ('OIG', 'obligor_industry_group'),
        ('OBNAME', 'obligor_name'),
    ))

    files_created = {}
    namespaces = ['r', 'rt']

    def __init__(self, csv_path):
        self.csv_path = csv_path
        self.db = sqlite3.connect('../var/db/ratings.sqlite3')

    def get_agency_id(self, name):
        c = self.db.cursor()
        c.execute('SELECT id FROM ratings_agency WHERE name=?', (name,))
        row = c.fetchone()
        if row:
            return row[0]
        c.execute('INSERT INTO ratings_agency (name, position) VALUES (?, 1)', (name,))
        self.db.commit()
        return c.lastrowid

    def save_file_record(self, path, agency_name):
        c = self.db.cursor()
        c.execute('SELECT id FROM ratings_file WHERE path=?', (path,))
        row = c.fetchone()
        with open(path, 'r') as f:
            lines_count = sum(1 for line in f)
        if row:
            c.execute('UPDATE ratings_file SET lines_count=? WHERE id=?', (lines_count, row[0]))
        else:
            agency_id = self.get_agency_id(agency_name)
            c.execute(
                'INSERT INTO ratings_file(path, agency_id, lines_count) VALUES (?, ?, ?)',
                (path, agency_id, lines_count)
            )
        self.db.commit()

    @staticmethod
    def get_value(d, key):
        for k in d:
            if ':' in k:
                key_found = k.split(':')[1] == key
            else:
                key_found = k == key
            if key_found:
                return d[k]
        return ''

    def get_value_without_namespace(self, d, keys):
        for key in keys:
            try:
                return d[key]
            except KeyError:
                for ns in self.namespaces:
                    try:
                        return d['{}:{}'.format(ns, key)]
                    except KeyError:
                        continue
        return 'NA'

    def export(self, row):
        dt = dateparser.parse(self.get_value_without_namespace(row, ['FCD']))
        asset_class = self.get_value_without_namespace(row, ['SSC', 'OSC'])
        agency = self.get_value_without_namespace(row, ['RAN'])
        file_name = '{} {} {}.csv'.format(
            dt.strftime('%Y%m%d'),
            agency, asset_class
        )
        if file_name in self.files_created:
            csv_file = self.files_created[file_name]['file']
            writer = self.files_created[file_name]['writer']
        else:
            self.files_created[file_name] = {}
            csv_file = self.files_created[file_name]['file'] = open(os.path.join(self.csv_path, file_name), 'w', encoding='utf8')
            writer = self.files_created[file_name]['writer'] = csv.writer(csv_file)
            self.files_created[file_name]['agency'] = agency
            writer.writerow([v for v in self.column_names_map.values()])
        writer.writerow([self.get_value(row, key) for key in self.column_names_map])
        csv_file.flush()

    def close(self):
        for v in self.files_created.values():
            v['file'].close()
            print(f"{v['file'].name=} {v['agency']=}")
            self.save_file_record(os.path.realpath(v['file'].name), v['agency'])


class NotLoggedInException(Exception):
    pass


class Downloader:
    selectors = {
        'moodies': {
            'login': 'div.login-name:nth-child(1) > div:nth-child(2) > input:nth-child(1)',
            'password': 'div.login-name:nth-child(2) > div:nth-child(2) > input:nth-child(1)',
            'submit': '.kIEpAt',
            'download': '.PageContent a:last-child',
            'accept': '#onetrust-accept-btn-handler',
        },
        'standardandpoors': {
            'login': '#username01',
            'password': '#password01',
            'submit': '.button-text',
            'accept_download': '#onetrust-accept-btn-handler',
            'download': [
                'div.table-module__row:nth-child(1) > div:nth-child(1) > p:nth-child(1) > span:nth-child(1)',
                'div.table-module__row:nth-child(2) > div:nth-child(1) > p:nth-child(1) > span:nth-child(1)',
                'div.table-module__row:nth-child(3) > div:nth-child(1) > p:nth-child(1) > span:nth-child(1)',
                'div.table-module__row:nth-child(4) > div:nth-child(1) > p:nth-child(1) > span:nth-child(1)',
                'div.table-module__row:nth-child(5) > div:nth-child(1) > p:nth-child(1) > span:nth-child(1)',
                'div.table-module__row:nth-child(6) > div:nth-child(1) > p:nth-child(1) > span:nth-child(1)',
                'div.table-module__row:nth-child(7) > div:nth-child(1) > p:nth-child(1) > span:nth-child(1)',
                'div.table-module__row:nth-child(1) > div:nth-child(2) > p:nth-child(1) > span:nth-child(1)',
                'div.table-module__row:nth-child(2) > div:nth-child(2) > p:nth-child(1) > span:nth-child(1)',
                'div.table-module__row:nth-child(3) > div:nth-child(2) > p:nth-child(1) > span:nth-child(1)',
                'div.table-module__row:nth-child(4) > div:nth-child(2) > p:nth-child(1) > span:nth-child(1)',
                'div.table-module__row:nth-child(5) > div:nth-child(2) > p:nth-child(1) > span:nth-child(1)',
                'div.table-module__row:nth-child(6) > div:nth-child(2) > p:nth-child(1) > span:nth-child(1)'
            ],
        },
        'fitchratings': {
            'accept': '#_evidon-accept-button',
            'login': False,
            'password': False,
            'submit': False,
            'download': '#btn-1',
        },
        'kbra': {
            'login': '#username',
            'password': '#password',
            'submit': '#login-button-label',
            'download': 'button.kbra-btn:nth-child(1)',
            'accept': False,
        },
        'dbrs': {
            'accept': '.button--fullWidth',
            'sign_in': 'body > app-root > app-site-header > div > nav.main-nav > div > ul > li.main-nav-item--mobile.login-btn.ng-star-inserted > a',
            'login': '#usernameField',
            'password': '#passwordField',
            'submit': '#btn-login',
            'accept_download': '#acceptance-check-box',
            'download': 'button.button',
        },
        'morningstar': {
            'download': '.nano-content>div>div>ul a',
        },
        'eganjones': {
            'download': '#rule_17g-7 a',
        },
        'hrratings': {
            'login': False,
            'link': True
        },
        'ambest': {
            'login': '#EMAIL',
            'password': '#CurPwd',
            'submit': '#btnContinue',
            'download': 'table.styledTable a',
        },
        'jcr': {
            'login': False,
            'password': False,
            'submit': False,
            'download': '.zip > a:nth-child(1)',
        },
    }

    def __init__(self, config):

        direct = os.path.dirname(os.path.abspath(__file__)).split('/')
        direct = '/'.join(direct[:-1])
        self.downloads_path = direct + config.get('general', 'downloads_path', fallback='../tmp/downloads/')
        self.config = config

        chrome_options = webdriver.ChromeOptions()
        prefs = {"download.default_directory": self.downloads_path,
                 "download.prompt_for_download": False,
                 "download.directory_upgrade": True}
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_experimental_option('prefs', prefs)
        self.browser = webdriver.Chrome(executable_path=ChromeDriverManager().install(), chrome_options=chrome_options)
        params = {'behavior': 'allow', 'downloadPath': self.downloads_path}
        self.browser.execute_cdp_cmd('Page.setDownloadBehavior', params)

        self.browser.implicitly_wait(10)
        logging.debug('Browser started')

    def is_download_completed(self):
        while True:
            time.sleep(10)
            current_downloads = glob.glob(self.downloads_path + '*.crdownload')
            print(f"{current_downloads=}")
            if len(current_downloads) == 0:
                break

    def login(self, agency):
        try:
            login = self.config[agency]['login']
            password = self.config[agency]['password']
        except KeyError as e:
            logging.debug('{} not provided for {}, skipping...'.format(e.args[0], agency))
            raise NotLoggedInException

        if 'accept' in self.selectors[agency]:
            try:
                self.browser.find_element(By.CSS_SELECTOR, self.selectors[agency]['accept']).click()
            except Exception as e:
                print(e)
        try:
            self.browser.find_element(By.CSS_SELECTOR, self.selectors[agency]['sign_in']).click()
        except Exception as e:
            print(e)

        if self.selectors[agency]['login']:
            WebDriverWait(self.browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, self.selectors[agency]['login'])))
            self.browser.find_element(By.CSS_SELECTOR, self.selectors[agency]['login']).send_keys(login)
        if self.selectors[agency]['password']:
            self.browser.find_element(By.CSS_SELECTOR, self.selectors[agency]['password']).send_keys(password)
        if self.selectors[agency]['submit']:
            time.sleep(5)
            self.browser.find_element(By.CSS_SELECTOR, self.selectors[agency]['submit']).click()

    def download(self, agency):
        paths = []
        login = False
        try:
            path = self.config[agency]['path']
        except KeyError as e:
            if e.args[0] == agency:
                logging.debug('{} section not provided in config file, skipping...'.format(agency))
            else:
                logging.debug('{} not provided for {}, skipping...'.format(e.args[0], agency))
        downloads_before = glob.glob(self.downloads_path + '*.zip')
        print(f"{downloads_before=}")
        print(path.split('\n'))
        for step in path.split('\n'):
            print(f"{step=}")
            if step == 'login':
                try:
                    self.login(agency)
                    login = True
                    if 'click_accept' in path.split('\n'):
                        login = False
                except NotLoggedInException:
                    return
            elif step == 'scroll_down':
                self.browser.execute_script('window.scrollTo(0, document.body.scrollHeight)')
            elif step == 'click_accept':
                self.browser.find_element(By.CSS_SELECTOR, self.selectors[agency]['accept_download']).click()
                time.sleep(1)
                login = True
            else:
                self.browser.get(step)
                if 'link' in self.selectors[agency]:
                    self.is_download_completed()
                if 'accept' in self.selectors[agency]:
                    try:
                        self.browser.find_element(By.CSS_SELECTOR, self.selectors[agency]['accept']).click()
                    except Exception as e:
                        print(e)

            if login or (not login and not self.selectors[agency]['login']):
                try:
                    print("begin download")
                    if 'download' in self.selectors[agency] and type(self.selectors[agency]['download']) is str:
                        WebDriverWait(self.browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, self.selectors[agency]['download'])))
                        time.sleep(5)
                        self.browser.find_element(By.CSS_SELECTOR, self.selectors[agency]['download']).click()
                        self.is_download_completed()
                    if 'download' in self.selectors[agency] and type(self.selectors[agency]['download']) is list:
                        for downl in self.selectors[agency]['download']:
                            print(downl)
                            WebDriverWait(self.browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, downl)))
                            self.browser.find_element(By.CSS_SELECTOR, downl).click()
                            self.is_download_completed()

                except Exception as e:
                    print(e)
                    print(f"{agency} trying FAIL!")
                    return False

            time.sleep(3)
        time.sleep(1)
        downloads_after = glob.glob(self.downloads_path + '*.zip')
        print(f"{downloads_after=}")
        for path in downloads_after:
            if path not in downloads_before:
                paths.append(path)

        return paths


def dict_to_list(d, row_template, rows):
    for key in d:
        if isinstance(d[key], dict) or isinstance(d[key], OrderedDict):#OrderedDict
            if '#text' in d[key]:
                row_template[key] = d[key]['#text']
            else:
                dict_to_list(d[key], row_template.copy(), rows)
    if row_template not in rows:
        rows.append(row_template)
    for key in d:
        if isinstance(d[key], list):
            for sub_d in d[key]:
                dict_to_list(sub_d, row_template.copy(), rows)


def parse_xml(file_path):
    with open(file_path, 'rb') as f:
        g = f.read()
        try:
            dict_data = xmltodict.parse(g)
        except:
            return []
    list_data_raw = []
    print(file_path)
    try:
        target_data = dict_data['xbrli:xbrl']['ROCRA']
    except KeyError:
        try:
            target_data = dict_data['xbrli:xbrl']['r:ROCRA']
        except KeyError:
            try:
                target_data = dict_data['xbrli:xbrl']['rt:ROCRA']
            except KeyError:
                target_data = dict_data['xbrli:xbrl']['ISD']
    dict_to_list(target_data, OrderedDict(), list_data_raw)
    max_len = max([len(r) for r in list_data_raw])
    list_data = [r for r in list_data_raw if len(r) == max_len]
    if 'FCD' not in list_data[0] and 'r:FCD' not in list_data[0] and 'rt:FCD' not in list_data[0]:
        fcd = dict_data['xbrli:xbrl']['FCD']['#text']
        for row in list_data:
            row['FCD'] = fcd
    if 'RAN' not in list_data[0] and 'r:RAN' not in list_data[0] and 'rt:RAN' not in list_data[0]:
        ran = dict_data['xbrli:xbrl']['RAN']['#text']
        for row in list_data:
            row['RAN'] = ran
    return list_data

def process_zip_file(file_path, source, exporter):
    logging.debug('{} zip file downloaded to {}'.format(source, file_path))
    zip_file = zipfile.ZipFile(file_path)
    for file_name in zip_file.namelist():
        if file_name.endswith('.xml'):
            try:
                extracted_path = zip_file.extract(file_name, xml_path)
            except zipfile.BadZipFile:
                logging.debug('{} extraction failed - archive corrupted!')
                continue
            logging.debug('{} extracted'.format(extracted_path))
            list_content = parse_xml(extracted_path)
            for row in list_content:
                exporter.export(row)
            logging.debug('{} parsed'.format(extracted_path))
    logging.debug('{} processed'.format(file_path))


def clear_dir(dir):
    for f in os.listdir(dir):
        path = os.path.join(dir, f)
        if os.path.isfile(path):
            os.unlink(path)
        else:
            shutil.rmtree(path)


if __name__ == '__main__':
    #time.sleep(1800)
    config = configparser.ConfigParser()
    config.read('conf.ini')

    logging.basicConfig(
        filename=config.get('general', 'log_file', fallback='rating_history_extracter.log'),
        filemode='a', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s'
    )

    downloads_path = config.get('general', 'downloads_path', fallback='../tmp/downloads/')
    if not os.path.exists(downloads_path):
        os.mkdir(downloads_path)
    elif not os.path.isdir(downloads_path):
        print('ERROR: downloads_path parameter points to file!')
        sys.exit(1)
    xml_path = config.get('general', 'xml_path', fallback='../tmp/xml_path/')
    if not os.path.exists(xml_path):
        os.mkdir(xml_path)
    elif not os.path.isdir(xml_path):
        print('ERROR: xml_path parameter points to file!')
        sys.exit(1)
    csv_path = config.get('general', 'csv_path', fallback='../tmp/csv_path/')
    if not os.path.exists(csv_path):
        os.mkdir(csv_path)
    elif not os.path.isdir(csv_path):
        print('ERROR: csv_path parameter points to file!')
        sys.exit(1)
    wipe_old_files = config.getboolean('general', 'wipe_old_files', fallback=False)
    if wipe_old_files:
        for dir in (downloads_path, xml_path):
            clear_dir(dir)

    logging.debug('Started')
    downloader = Downloader(config)
    exporter = CSVExporter(csv_path)
    #['moodies', 'standardandpoors', 'fitchratings', 'kbra' 'dbrs', 'ambest', 'hrratings', 'jcr']
    # ['moodies', 'standardandpoors', 'fitchratings', 'kbra' 'dbrs', 'hrratings', 'jcr']
    for agency in ['moodies', 'standardandpoors', 'fitchratings', 'kbra' 'dbrs', 'hrratings', 'jcr']:
        paths = downloader.download(agency)
        if paths == False:
            downloader = Downloader(config)
        print(f"{paths=}")
        if paths:
            if wipe_old_files:
                pass
            for path in paths:
                print(f"{path=}")
                process_zip_file(path, agency.capitalize, exporter)
    logging.debug('Rating history conversion finished!')
    downloader.browser.quit()
    exporter.close()
