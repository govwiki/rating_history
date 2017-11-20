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
        if ':' in key:
            key = key.split(':')[1]
        return d.get(key, '')

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
        file_name = '{}_{}_{}.csv'.format(
            dt.strftime('%Y%m%d'),
            agency, asset_class
        )
        if file_name in self.files_created:
            csv_file = self.files_created[file_name]['file']
            writer = self.files_created[file_name]['writer']
        else:
            self.files_created[file_name] = {}
            csv_file = self.files_created[file_name]['file'] = open(os.path.join(self.csv_path, file_name), 'w')
            writer = self.files_created[file_name]['writer'] = csv.writer(csv_file)
            writer.writerow([v for v in self.column_names_map.values()])
        writer.writerow([self.get_value(row, key).encode('utf8') for key in self.column_names_map])
        csv_file.flush()

    def close(self):
        for v in self.files_created.values():
            v['file'].close()


def is_download_completed(downloads_path):
    time.sleep(10)
    while True:
        current_downloads = glob.glob(downloads_path + '*.crdownload')
        if len(current_downloads) == 0:
            # print'Downloading ' + audit + ' completed')
            break
        else:
            time.sleep(10)


def download_moodies(browser, config):
    downloads_path = config.get('general', 'downloads_path', fallback='/tmp/downloads/')
    try:
        url = config['moodies']['url']
        login = config['moodies']['login']
        password = config['moodies']['password']
    except KeyError as e:
        if e.args[0] == 'moodies':
            logging.debug('moodies section not provided in config file, skipping...')
        else:
            logging.debug('{} not provided for moodies, skipping...'.format(e.args[0]))
        return
    downloads_before = glob.glob(downloads_path + '*.zip')
    browser.get(url)
    browser.find_element_by_css_selector('#mdcLoginControl #MdcUserName').send_keys(login)
    browser.find_element_by_css_selector('#mdcLoginControl #MdcPassword').send_keys(password)
    browser.find_element_by_css_selector('#mdcLoginControl #LoginImageButton').click()
    browser.find_element_by_link_text('Click here').click()
    is_download_completed(downloads_path)
    downloads_after = glob.glob(downloads_path + '*.zip')
    for path in downloads_after:
        if path not in downloads_before:
            return path


def download_standard_and_poors(browser, config):
    downloads_path = config.get('general', 'downloads_path', fallback='/tmp/downloads/')
    try:
        url = config['standardandpoors']['url']
        login = config['standardandpoors']['login']
        password = config['standardandpoors']['password']
    except KeyError as e:
        if e.args[0] == 'standardandpoors ':
            logging.debug('standardandpoors section not provided in config file, skipping...')
        else:
            logging.debug('{} not provided for standardandpoors, skipping...'.format(e.args[0]))
        return []
    downloads_before = glob.glob(downloads_path + '*.zip')
    browser.get(url)
    browser.find_element_by_id('_oamloginportlet_WAR_rdsmregistrationportlet_email').send_keys(login)
    browser.find_element_by_id('_oamloginportlet_WAR_rdsmregistrationportlet_password').send_keys(password)
    browser.find_element_by_id('submitForm').click()
    time.sleep(10)
    for link in browser.find_elements_by_css_selector('div.ratings-history-files a'):
        link.click()
        is_download_completed(downloads_path)
    downloads_after = glob.glob(downloads_path + '*.zip')
    for path in downloads_after:
        if path not in downloads_before:
            yield path


def download_fitchratings(browser, config):
    downloads_path = config.get('general', 'downloads_path', fallback='/tmp/downloads/')
    try:
        url = config['fitchratings']['url']
        login = config['fitchratings']['login']
        password = config['fitchratings']['password']
    except KeyError as e:
        if e.args[0] == 'fitchratings':
            logging.debug('fitchratings section not provided in config file, skipping...')
        else:
            logging.debug('{} not provided for moodies, skipping...'.format(e.args[0]))
        return
    downloads_before = glob.glob(downloads_path + '*.zip')
    browser.get(url)
    browser.find_element_by_link_text('17g-7 Ratings History Disclosure').click()
    browser.switch_to.window(browser.window_handles[1])
    browser.find_element_by_id('loginForm:userName').send_keys(login)
    browser.find_element_by_id('loginForm:password').send_keys(password)
    browser.find_element_by_id('loginForm:submit').click()
    browser.find_element_by_id('license:accept').click()
    is_download_completed(downloads_path)
    downloads_after = glob.glob(downloads_path + '*.zip')
    for path in downloads_after:
        if path not in downloads_before:
            return path


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
    try:
        target_data = dict_data['xbrli:xbrl']['ROCRA']
    except KeyError:
        try:
            target_data = dict_data['xbrli:xbrl']['r:ROCRA']
        except KeyError:
            target_data = dict_data['xbrli:xbrl']['rt:ROCRA']
    dict_to_list(target_data, OrderedDict(), list_data_raw)
    max_len = max([len(r) for r in list_data_raw])
    list_data = [r for r in list_data_raw if len(r) == max_len]
    return list_data


def process_zip_file(file_path, source):
    exporter = CSVExporter(csv_path)
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
    exporter.close()
    logging.debug('{} processed'.format(file_path))


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
    if config.getboolean('general', 'wipe_old_files', fallback=False):
        for dir in (downloads_path, xml_path, csv_path):
            for f in os.listdir(dir):
                path = os.path.join(dir, f)
                if os.path.isfile(path):
                    os.unlink(path)
                else:
                    shutil.rmtree(path)

    logging.debug('Started')
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox") 
    prefs = {"download.default_directory": downloads_path}
    options.add_experimental_option("prefs", prefs)
    browser = webdriver.Chrome(chrome_options=options, service_args=["--verbose", "--log-path=/tmp/selenium.log"])
    browser.implicitly_wait(10)
    logging.debug('Browser started')

    moodies_zip_path = download_moodies(browser, config)
    if moodies_zip_path:
        process_zip_file(moodies_zip_path, 'Moodies')

    for zip_path in download_standard_and_poors(browser, config):
        process_zip_file(zip_path, 'Standardandpoors')

    fitchratings_zip_path = download_fitchratings(browser, config)
    if fitchratings_zip_path:
        process_zip_file(fitchratings_zip_path, 'Fitchratings')
    logging.debug('Rating history conversion finished!')
    browser.quit()
