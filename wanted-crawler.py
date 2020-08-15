import requests
from bs4 import BeautifulSoup
from selenium import webdriver

from contextlib import closing
from multiprocessing import Pool, Manager
from itertools import repeat

import re
import time, csv
import json
from pymongo import MongoClient


def insertDocument(chunk):
    client = MongoClient('localhost', 27000)
    db = client.wanted
    recruitInfo = db['recruitInfo']
    recruitInfo.insert_many(chunk)


def getJobGroups():
    res = requests.get(
        'https://www.wanted.co.kr/wdlist/518?country=kr&job_sort=job.latest_order&years=-1&locations=all')
    html = res.text
    soup = BeautifulSoup(html, "html.parser")

    jobGroups = []
    for elements in soup.find("div", class_="_2h5Qtv_8mK2LOH-yR3FTRs").find_all("li"):
        href = elements.find("a")["href"]
        span = elements.find("span")
        jobGroup = {span.get_text(): "https://www.wanted.co.kr" + href}
        jobGroups.append(jobGroup)
        print(jobGroup)
    return jobGroups


def connectWebDriver(web):
    options = webdriver.ChromeOptions()
    options.add_argument("disable-gpu")
    options.add_argument("headless")
    options.add_argument("lang=ko_KR")

    # 브라우저 화면 크기에 따라 미디어 쿼리 등에 따라 element 구조가
    # 달라질 수 있으므로 고정시키고 시작하기
    options.add_argument('--start-maximized')

    options.add_argument(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36")

    driver = webdriver.Chrome('chromedriver/chromedriver', options=options)
    # 헤더 탐지 피하기
    driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: function() {return[1, 2, 3, 4, 5];},});")
    driver.execute_script("Object.defineProperty(navigator, 'languages', {get: function() {return ['ko-KR', 'ko']}})")
    driver.execute_script(
        "const getParameter = WebGLRenderingContext.getParameter;WebGLRenderingContext.prototype.getParameter = function(parameter) {if (parameter === 37445) {return 'NVIDIA Corporation'} if (parameter === 37446) {return 'NVIDIA GeForce GTX 980 Ti OpenGL Engine';}return getParameter(parameter);};")

    driver.implicitly_wait(2)
    driver.get(web)
    driver.implicitly_wait(2)

    return driver


def scrollPage(driver):
    SCROLL_PAUSE_TIME = 0.5
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE_TIME)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight-50);")
        time.sleep(SCROLL_PAUSE_TIME)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height


def getRecruitInfoList(urlDict, recruitInfos):
    driver = connectWebDriver(''.join(urlDict.values()))

    scrollPage(driver)

    allRecruitInfo = driver.find_elements_by_xpath('//div[@class="_3D4OeuZHyGXN7wwibRM5BJ"]/a')

    if allRecruitInfo:
        for recruitInfo in allRecruitInfo:
            group = ''.join(urlDict.keys())
            recruitInfoUrl = recruitInfo.get_attribute('href')
            recruitInfos[group].append(recruitInfoUrl)
            print(group, recruitInfoUrl)

    driver.quit()


def getRecruitInfo(urlList, allRecruitInfo):
    group, url = urlList
    print(group, url)

    try:
        driver = connectWebDriver(url)
    except Exception as error:
        print('**************** Driver Connection 에러 발생 *********************')
        print(group, url)
        print(error.args)
        with open(r'data/ErrorHistory.json', 'a', encoding='utf-8', newline='') as file:
            error = {
                'title': f'Driver Connection Error',
                'group': f'{group}',
                'url': f'{url}',
                'detail': error.args
            }
            json.dump(error, file, indent=4, ensure_ascii=False)
            file.write('\n,')
        print('****************************************************************')
        return

    whereElement, tagElements, companyElement, detailElements, deadlineElement, workAreaElement = '', '', '', '', '', ''
    try:
        whereElement = driver.find_element_by_xpath('/html/body/div[1]/div/div[3]/div[1]/div[1]/div/section[2]/div[1]/span')
        tagElements = driver.find_elements_by_xpath('//div[@class="ObubI7m2AFE5fxlR8Va9t"]/ul/li/a')
        companyElement = driver.find_element_by_xpath('//section[@class="Bfoa2bzuGpxK9ieE1GxhW"]/div/h6/a')
        detailElements= driver.find_elements_by_xpath('//section[@class="_1LnfhLPc7hiSZaaXxRv11H"]/p')
        deadlineElement = driver.find_element_by_xpath('/html/body/div[1]/div/div[3]/div[1]/div[1]/div[1]/div[2]/section[2]/div[1]/span[2]')
        workAreaElement = driver.find_element_by_xpath('/html/body/div[1]/div/div[3]/div[1]/div[1]/div[1]/div[2]/section[2]/div[2]/span[2]')

    except Exception as error:
        print('\n**************** Element Find 에러발생 *********************')
        print(group, url)
        print(error.args)
        with open(r'data/ErrorHistory.json', 'a', encoding='utf-8', newline='') as file:
            error = {
                'title': 'Driver Connection Error',
                'group': f'{group}',
                'url': f'{url}',
                'detail': error.args
            }
            json.dump(error, file, indent=4, ensure_ascii=False)
            file.write('\n,')
        print('****************************************************************\n')

    pattern = "[∙\n•#\"!:)/]"
    id = url.replace('https://www.wanted.co.kr/wd/', '')
    region, country = whereElement.text.split('\n.\n') if whereElement else [None, None]
    tags = [re.sub(pattern=pattern, repl='', string=tagElement.text) for tagElement in tagElements] \
        if tagElements else []
    company = companyElement.text if companyElement else None
    details = [re.sub(pattern=pattern, repl='', string=detailElement.text).strip()\
                   for detailElement in detailElements] if detailElements else []
    deadline = deadlineElement.text if deadlineElement else None
    workArea = workAreaElement.text if workAreaElement else None

    contents = []
    contents.append(id)
    contents.append(group)
    contents.append(region)
    contents.append(country)
    contents.append(tags)
    contents.append(company)
    contents.extend(details)
    contents.append(deadline)
    contents.append(workArea)

    headers = ['id', '직군', '지역', '국가', '태그', '회사명', '회사소개', '주요업무', '자격요건', '우대사항', '혜택 및 복지', '마감일', '근무지']

    recruitInfoDict = {header: value for header, value in zip(headers, contents)}
    allRecruitInfo.append(recruitInfoDict)
    print(recruitInfoDict)

    with open('data/RecruitInfo.json', 'a', encoding='UTF-8') as file:
        json.dump(recruitInfoDict, file, indent=4, ensure_ascii=False)
        file.write('\n,')

    driver.quit()


def scrapRecruitList(groups):
    recruitInfosByGroup = manager.dict()

    for group in groups:
        recruitInfosByGroup[''.join(group.keys())] = manager.list()

    with closing(Pool(processes=7)) as pool:
        pool.starmap(getRecruitInfoList, zip(groups, repeat(recruitInfosByGroup)))

    with open(r'data/RecruitInfoList.csv', 'w', encoding='utf-8', newline='') as file:
        RecruitInfoListfile = csv.writer(file)
        for group, urls in recruitInfosByGroup.items():
            for url in urls:
                RecruitInfoListfile.writerow([group, url])
    print('직군별 채용공고리스트 url 저장 완료!')


def scrapRecruitInfo():
    with open('data/RecruitInfoList.csv', 'r', encoding='UTF-8') as file:
        RecruitInfoListReader = csv.reader(file)
        RecruitInfoList = []
        for RecruitInfo in RecruitInfoListReader:
            if len(RecruitInfo) == 2:
                RecruitInfoList.append(RecruitInfo)

    # RecruitInfoList = [['블록체인 개발자', 'https://www.wanted.co.kr/wd/39895'],
    #                    ['서버 개발자', 'https://www.wanted.co.kr/wd/16854']]

    allRecruitInfo = manager.list()

    with open('data/RecruitInfo.json', 'w', encoding='UTF-8') as file:
        file.write('[\n')

    with open('data/ErrorHistory.json', 'w', encoding='UTF-8') as file:
        file.writelines('[\n')

    with closing(Pool(processes=7)) as pool:
        pool.starmap(getRecruitInfo, zip(RecruitInfoList, repeat(allRecruitInfo)))

    recruitInfoFile = open('data/RecruitInfo.json', 'r', encoding='UTF-8')
    recruitInfolines = recruitInfoFile.readlines()
    recruitInfoFile.close()

    errorInfoFile = open('data/ErrorHistory.json', 'r', encoding='UTF-8')
    errorInfoFilelines = errorInfoFile.readlines()
    errorInfoFile.close()

    with open('data/RecruitInfo.json', 'w', encoding='UTF-8') as file:
        file.writelines([item for item in recruitInfolines[:-1]])
        file.write(']')

    with open('data/ErrorHistory.json', 'w', encoding='UTF-8') as file:
        file.writelines([item for item in errorInfoFilelines[:-1]])
        file.write(']')

    # with open('data/recruitInfo.json', 'w', encoding='UTF-8') as file:
    #     json.dump(list(allRecruitInfo), file, indent=4, ensure_ascii=False)


if __name__ == '__main__':
    manager = Manager()

    print('---------채용직군---------------------------')
    jobGroups = getJobGroups()

    print('---------채용공고리스트----------------------')
    scrapRecruitList(jobGroups)

    print('---------채용공고---------------------------')
    scrapRecruitInfo()
