from pyvirtualdisplay import Display
from selenium import webdriver
from collections import OrderedDict
import xmltodict
import time
import os
import csv
import sys
import ntpath
import logging
import zipfile
import glob
import configparser


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


def get_keys(lst):
    keys = []
    for row in lst:
        for key in row:
            if key not in keys:
                keys.append(key)
    return keys


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
    with open(file_path, 'r') as f:
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
    logging.debug('{} zip file downloaded to {}'.format(source, file_path))
    zip_file = zipfile.ZipFile(file_path)
    list_content = []
    for file_name in zip_file.namelist():
        if file_name.endswith('.xml'):
            extracted_path = zip_file.extract(file_name, xml_path)
            logging.debug('{} extracted'.format(extracted_path))
            list_content += parse_xml(extracted_path)
            logging.debug('{} parsed'.format(extracted_path))
    header = get_keys(list_content)
    csv_file_name = os.path.join(csv_path, ntpath.basename(file_name).replace('.xml', '.csv'))
    with open(csv_file_name, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for row in list_content:
            csv_row = []
            for key in header:
                csv_row.append(row.get(key, ''))
            writer.writerow(csv_row)
            f.flush()
    logging.debug('{} saved'.format(csv_file_name))


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
    csv_path = config.get('general', 'xml_path', fallback='/tmp/csv_path/')
    if not os.path.exists(csv_path):
        os.mkdir(csv_path)
    elif not os.path.isdir(csv_path):
        print('ERROR: xml_path parameter points to file!')
        sys.exit(1)

    logging.debug('Started')
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    prefs = {"download.default_directory": downloads_path}
    options.add_experimental_option("prefs", prefs)
    browser = webdriver.Chrome(chrome_options=options)
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
