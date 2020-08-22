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
from eunjeon import Mecab

client = MongoClient('1.222.84.186', 27017)
db = client.get_database('wanted')

def insertDB(collection, chunk):
    db[collection].insert(chunk, check_keys=False)

def readDB(collection):
    return list(db[collection].find())

def getJobGroups():
    res = requests.get(
        'https://www.wanted.co.kr/wdlist/518?country=kr&job_sort=job.latest_order&years=-1&locations=all')
    html = res.text
    soup = BeautifulSoup(html, "html.parser")

    jobGroups = []
    for elements in soup.find("div", class_="_2h5Qtv_8mK2LOH-yR3FTRs").find_all("li"):
        href = elements.find("a")["href"]
        span = elements.find("span")
        jobGroup = {'jobGroup': span.get_text(), 'url': "https://www.wanted.co.kr" + href}
        jobGroups.append(jobGroup)
        print(jobGroup)
    insertDB("jobGroupUrl", jobGroups)
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
    print(urlDict['jobGroup'])
    driver = connectWebDriver(urlDict['url'])

    scrollPage(driver)

    allRecruitInfo = driver.find_elements_by_xpath('//div[@class="_3D4OeuZHyGXN7wwibRM5BJ"]/a')

    if allRecruitInfo:
        for recruitInfo in allRecruitInfo:
            # group = ''.join(urlDict.keys())
            print(urlDict['jobGroup'])
            jobGroup = urlDict['jobGroup']
            recruitInfoUrl = recruitInfo.get_attribute('href')
            # recruitInfos[group].append(recruitInfoUrl)
            recruitInfo = {'jobGroup': jobGroup, 'url': recruitInfoUrl}
            recruitInfos.append(recruitInfo)
            insertDB("recruitInfoUrl", recruitInfo)
            print(jobGroup, recruitInfoUrl)

    driver.quit()

def errorFormatter(error, recruitInfoUrl):
    return {
                'where': recruitInfoUrl,
                'detail': error
            }

def saveError(errorFileDir, element, recruitInfoUrl):
    with open(errorFileDir, 'a', encoding='utf-8', newline='') as file:
        errorObject = errorFormatter(element, recruitInfoUrl)
        json.dump(errorObject, file, indent=4, ensure_ascii=False)
        file.write('\n,')


def getAllElement(driver, elementErrorDir, recruitInfoUrl):
    whereElement, tagElements, companyElement, detailElements, whereElement, deadlineElement, workAreaElement \
        = '', '', '', '', '', '', ''
    try:
        companyElement = driver.find_element_by_xpath('//section[@class="Bfoa2bzuGpxK9ieE1GxhW"]/div/h6/a')
    except Exception:
        saveError(elementErrorDir, 'companyElement', recruitInfoUrl)
    try:
        detailElements = driver.find_elements_by_xpath('//section[@class="_1LnfhLPc7hiSZaaXxRv11H"]/p')
    except Exception:
        saveError(elementErrorDir, 'detailElements', recruitInfoUrl)
    try:
        tagElements = driver.find_elements_by_xpath('//div[@class="ObubI7m2AFE5fxlR8Va9t"]/ul/li/a')
    except Exception:
        saveError(elementErrorDir, 'tagElements', recruitInfoUrl)
    try:
        whereElement = driver.find_element_by_xpath(
            '/html/body/div[1]/div/div[3]/div[1]/div[1]/div/section[2]/div[1]/span')
    except Exception:
        saveError(elementErrorDir, 'whereElement', recruitInfoUrl)
    try:
        workAreaElement = driver.find_element_by_xpath(
            '/html/body/div[1]/div/div[3]/div[1]/div[1]/div[1]/div[2]/section[2]/div[2]/span[2]')
    except Exception:
        saveError(elementErrorDir, 'workAreaElement', recruitInfoUrl)
    try:
        deadlineElement = driver.find_element_by_xpath(
            '/html/body/div[1]/div/div[3]/div[1]/div[1]/div[1]/div[2]/section[2]/div[1]/span[2]')
    except Exception:
        saveError(elementErrorDir, 'deadlineElement', recruitInfoUrl)

    return [whereElement, tagElements, companyElement, detailElements, whereElement, deadlineElement, workAreaElement]


def getInfosByElements(elements):
    pattern = "[∙\n•#\"!:)/■❤♥ ️▶✔▪^회사소개]"
    region, country = elements[0].text.split('\n.\n') if elements[0] else [None, None]
    tags = [re.sub(pattern=pattern, repl='', string=tagElement.text) for tagElement in elements[1]] \
        if elements[1] else []
    company = elements[2].text if elements[2] else None
    details = [re.sub(pattern=pattern, repl='', string=detailElement.text).strip() \
               for detailElement in elements[3]] if elements[3] else []
    workArea = elements[4].text if elements[4] else None
    deadline = elements[5].text if elements[5] else None
    return region, country, tags, company, details, workArea, deadline


def createrecruitInfo(contents):
    # headers = ['id', '직군', '지역', '국가', '태그', '회사명', '회사소개', '주요업무', '자격요건', '우대사항', '혜택 및 복지', '마감일', '근무지']
    headers = ['직군', '지역', '국가', '태그', '회사명', '회사소개', '주요업무', '자격요건', '우대사항', '혜택 및 복지', '마감일', '근무지', '회사소개_명사', '주요업무_명사', '자격요건_명사', '우대사항_명사', '혜택 및 복지_명사']
    recruitInfo = {header: value for header, value in zip(headers, contents)}
    # with open('data/RecruitInfoLog.json', 'a', encoding='UTF-8') as file:
    #     json.dump(recruitInfo, file, indent=4, ensure_ascii=False)
    #     file.write('\n,')
    return recruitInfo


def getRecruitInfo(recruitInfoUrl, allRecruitInfo, connectedErrorDir, elementErrorDir):
    mecab = Mecab()
    group = recruitInfoUrl['jobGroup']
    url = recruitInfoUrl['url']
    print(group, url)

    contents = []
    try:
        driver = connectWebDriver(url)
    except Exception as error:
        saveError(connectedErrorDir, error, recruitInfoUrl)
        return

    recruitInfoElements = getAllElement(driver, elementErrorDir, recruitInfoUrl)

    region, country, tags, company, details, workArea, deadline = getInfosByElements(recruitInfoElements)

    contents.append(group)
    contents.append(region)
    contents.append(country)
    contents.append(tags)
    contents.append(company)
    contents.extend(details)
    contents.append(deadline)
    contents.append(workArea)
    for detail in details:
        contents.append(mecab.nouns(detail))
    recruitInfo = createrecruitInfo(contents)
    allRecruitInfo.append(recruitInfo)

    insertDB("recruitInfo", recruitInfo)

    print('완료! ', recruitInfo)
    driver.quit()


def scrapRecruitList(groups):
    recruitInfosByGroup = manager.list()
    with closing(Pool(processes=7)) as pool:
        pool.starmap(getRecruitInfoList, zip(groups, repeat(recruitInfosByGroup)))

    # insertDB("recruitInfoUrl", recruitInfosByGroup)
    print('직군별 채용공고리스트 url 저장 완료!')

    return recruitInfosByGroup


def openJsonFile(fileDir):
    with open(fileDir, 'w', encoding='UTF-8') as file:
        file.writelines('[\n')


def closeJsonFile(fileDir):
    errorInfoFile = open(fileDir, 'r', encoding='UTF-8')
    errorInfoFilelines = errorInfoFile.readline()
    errorInfoFile.close()
    with open(fileDir, 'w', encoding='UTF-8') as file:
        file.writelines([item for item in errorInfoFilelines[:-1]])
        file.write(']')


def checkError(fileDir):
    errorInfoFile = open(fileDir, 'r', encoding='UTF-8')
    errorInfoFilelines = json.load(errorInfoFile)
    errorInfoFile.close()
    RecruitInfoList = [errorInfoFileline['where'] for errorInfoFileline in errorInfoFilelines] if len(errorInfoFilelines) > 0 else []
    return RecruitInfoList


def getRecruitInfoURLs():
    RecruitInfoList = []
    with open('data/RecruitInfoList.csv', 'r', encoding='UTF-8') as file:
        RecruitInfoListReader = csv.reader(file)
        for RecruitInfo in RecruitInfoListReader:
            if len(RecruitInfo) == 2:
                RecruitInfoList.append(RecruitInfo)
    return RecruitInfoList


def scrapRecruitInfo(recruitInfoURLs):
    recruitInfoLogsDir = 'data/RecruitInfoLog.json'
    elementErrorDir = f'data/errors/FindElementError.json'
    connectedErrorDir = f'data/errors/ConnectedError.json'
    openJsonFile(recruitInfoLogsDir)
    openJsonFile(elementErrorDir)

    allRecruitInfo = manager.list()

    while len(recruitInfoURLs) > 0:
        openJsonFile(connectedErrorDir)
        with closing(Pool(processes=7)) as pool:
            pool.starmap(getRecruitInfo, zip(recruitInfoURLs, repeat(allRecruitInfo), repeat(connectedErrorDir),
                                             repeat(elementErrorDir)))

        closeJsonFile(connectedErrorDir)
        recruitInfoURLs = checkError(connectedErrorDir)

    closeJsonFile(elementErrorDir)
    closeJsonFile(recruitInfoLogsDir)
    # insertDB("recruitInfo", allRecruitInfo)
    # with open('data/RecruitInfo.json', 'w', encoding='UTF-8') as file:
    #     json.dump(list(allRecruitInfo), file, indent=4, ensure_ascii=False)

    print('채용상세정보 수집 모두 완료!')


if __name__ == '__main__':
    manager = Manager()

    print('---------채용직군---------------------------')
    jobGroups = getJobGroups()
    # #
    print('---------채용공고리스트----------------------')
    recruitInfosByGroup = scrapRecruitList(jobGroups)
    # testrecruitInfosByGroup = scrapRecruitList([{'jobGroup': '서버 개발자', 'url': "https://www.wanted.co.kr/wdlist/518/872"}])
    # print(testrecruitInfosByGroup)

    print('---------채용공고---------------------------')
    # scrapRecruitInfo()

    recruitInfosByGroup = readDB('recruitInfoUrl')
    # recruitInfosByGroup = [{'jobGroup': '서버 개발자', 'url': 'https://www.wanted.co.kr/wd/42966'}]
    scrapRecruitInfo(recruitInfosByGroup)
    # testscrapRecruitInfo=scrapRecruitInfo([{'jobGroup': '서버 개발자', 'url': 'https://www.wanted.co.kr/wd/42966'}])
