from pyvirtualdisplay import Display
from selenium import webdriver
from collections import OrderedDict
import dateparser
import xmltodict
import time
import os
import csv
import sys
import ntpath
import logging
import shutil
import zipfile
import glob
import configparser


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
        return 'N/A'

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
            writer.writerow([v for v in self.column_names_map.values()])
        writer.writerow([self.get_value(row, key) for key in self.column_names_map])
        csv_file.flush()

    def close(self):
        for v in self.files_created.values():
            v['file'].close()


class NotLoggedInException(Exception):
    pass


class Downloader:
    selectors = {
        'moodies': {
            'login': '#mdcLoginControl #MdcUserName',
            'password': '#mdcLoginControl #MdcPassword',
            'submit': '#mdcLoginControl #LoginImageButton',
            'download': '.PageContent a:last-child',
        },
        'standardandpoors': {
            'login': '#_oamloginportlet_WAR_rdsmregistrationportlet_email',
            'password': '#_oamloginportlet_WAR_rdsmregistrationportlet_password',
            'submit': '#submitForm',
            'download': 'div.ratings-history-files a',
        },
        'fitchratings': {
            'login': '#loginForm:userName',
            'password': '#loginForm:password',
            'submit': '#loginForm:submit',
            'download': '#license:accept',
        },
        'krollbond': {
            'login': 'input[name="email"]',
            'password': 'input[name="password"]',
            'submit': 'button[type="submit"]',
            'download': '#SEC_regulartory_docs_list>ul:last-child a',
        },
        'dbrs': {
            'form': 'li.login-btn',
            'accept': 'input:last-child',
            'login': '#usernameField',
            'password': '#passwordField',
            'submit': 'a[ng-click="$ctrl.submit()"]',
            'download': 'button[ng-click="$ctrl.getXBRL()"]',
        },
        'morningstar': {
            'download': '.nano-content>div>div>ul a',
        },
        'eganjones': {
            'download': '#rule_17g-7 a',
        },
        'hrratings': {
            'download': 'div.col.wMini>div>p>a',
        },
        'ambest': {
            'login': '#EMAIL',
            'password': '#CurPwd',
            'submit': '#btnContinue',
            'download': 'table.styledTable a',
        },
        'jcr': {
            'download': 'td.zip a',
        },
    }

    def __init__(self, config):
        self.downloads_path = config.get('general', 'downloads_path', fallback='/tmp/downloads/')
        self.config = config
        options = webdriver.ChromeOptions()
        options.add_argument("--no-sandbox")
        prefs = {"download.default_directory": self.downloads_path}
        options.add_experimental_option("prefs", prefs)
        self.browser = webdriver.Chrome(chrome_options=options, service_args=["--verbose", "--log-path=/tmp/selenium.log"])
        self.browser.implicitly_wait(10)
        logging.debug('Browser started')

    def is_download_completed(self):
        while True:
            time.sleep(10)
            current_downloads = glob.glob(self.downloads_path + '*.crdownload')
            if len(current_downloads) == 0:
                break

    def login(self, agency):
        try:
            login = self.config[agency]['login']
            password = self.config[agency]['password']
        except KeyError as e:
            logging.debug('{} not provided for {}, skipping...'.format(e.args[0], agency))
            raise NotLoggedInException
        self.browser.find_element_by_css_selector(self.selectors[agency]['login']).send_keys(login)
        self.browser.find_element_by_css_selector(self.selectors[agency]['password']).send_keys(password)
        self.browser.find_element_by_css_selector(self.selectors[agency]['submit']).click()

    def download(self, agency):
        try:
            path = self.config[agency]['path']
        except KeyError as e:
            if e.args[0] == agency:
                logging.debug('{} section not provided in config file, skipping...'.format(agency))
            else:
                logging.debug('{} not provided for {}, skipping...'.format(e.args[0], agency))
        downloads_before = glob.glob(self.downloads_path + '*.zip')
        for step in path.split('\n'):
            if step == 'login':
                try:
                    self.login(agency)
                except NotLoggedInException:
                    return
            elif step == 'scroll_down':
                self.browser.execute_script('window.scrollTo(0, document.body.scrollHeight)')
            elif step == 'click_form':
                self.browser.find_element_by_css_selector(self.selectors[agency]['form']).click()
            elif step == 'click_accept':
                self.browser.find_element_by_css_selector(self.selectors[agency]['accept']).click()
            else:
                self.browser.get(step)
            time.sleep(3)
        for link in self.browser.find_elements_by_css_selector(self.selectors[agency]['download']):
            link.click()
            self.is_download_completed()
        downloads_after = glob.glob(self.downloads_path + '*.zip')
        for path in downloads_after:
            if path not in downloads_before:
                yield path


def dict_to_list(d, row_template, rows):
    for key in d:
        if isinstance(d[key], OrderedDict):
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
        dict_data = xmltodict.parse(f.read())
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
    list_content = []
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
    config = configparser.ConfigParser()
    config.read('conf.ini')

    logging.basicConfig(
        filename=config.get('general', 'log_file', fallback='rating_history_extracter.log'),
        filemode='a', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s'
    )

    if config.getboolean('general', 'headless_mode', fallback=True):
        display = Display(visible=0, size=(1920, 1080))
        display.start()

    downloads_path = config.get('general', 'downloads_path', fallback='/tmp/downloads/')
    if not os.path.exists(downloads_path):
        os.mkdir(downloads_path)
    elif not os.path.isdir(downloads_path):
        print('ERROR: downloads_path parameter points to file!')
        sys.exit(1)
    xml_path = config.get('general', 'xml_path', fallback='/tmp/xml_path/')
    if not os.path.exists(xml_path):
        os.mkdir(xml_path)
    elif not os.path.isdir(xml_path):
        print('ERROR: xml_path parameter points to file!')
        sys.exit(1)
    csv_path = config.get('general', 'csv_path', fallback='/tmp/csv_path/')
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
    for agency in ['moodies', 'standardandpoors', 'krollbond', 'dbrs', 'morningstar', 'eganjones', 'hrratings', 'ambest', 'jcr']:
        paths = downloader.download(agency)
        if paths:
            if wipe_old_files:
                pass
            for path in paths:
                print(path)
                process_zip_file(path, agency.capitalize, exporter)
    logging.debug('Rating history conversion finished!')
    downloader.browser.quit()
    exporter.close()
