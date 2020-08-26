import requests
from bs4 import BeautifulSoup
from selenium import webdriver

from contextlib import closing
from multiprocessing import Pool, Manager
from itertools import repeat

import re
from itertools import chain
import time, csv
import json
from pymongo import MongoClient

import nltk
from nltk.corpus import stopwords
from ckonlpy.tag import Twitter,Postprocessor
from ckonlpy.utils import load_wordset, load_ngram

# nltk.download('punkt')
# nltk.download('stopwords')
twitter = Twitter()
stopwordsKR = load_wordset('cleansing_data/korean_stopwords.txt', encoding='ANSI')
customStopwordsEN = load_wordset('cleansing_data/english_stopwords.txt', encoding='ANSI')
stopwordsEN = customStopwordsEN.union(set(stopwords.words('english')))
ngrams = load_ngram('cleansing_data/korean_ngram.txt')
userdicts = load_wordset('cleansing_data/korean_user_dict.txt')
twitter.add_dictionary(list(userdicts), 'Noun', force=True)

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
    # insertDB("jobGroupUrl", jobGroups)
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
            jobGroup = urlDict['jobGroup']
            recruitInfoUrl = recruitInfo.get_attribute('href')
            recruitInfo = {'jobGroup': jobGroup, 'url': recruitInfoUrl}
            recruitInfos.append(recruitInfo)
            with open('data/recruitInfoList.csv', 'a', encoding='utf-8-sig', newline='') as file:
                writer = csv.writer(file)
                writer.writerow([jobGroup, recruitInfoUrl])
    driver.quit()


def saveError(type, recruitInfoUrl, error):
    errorlog = {'type': type, 'where': recruitInfoUrl, 'detail': error}
    with open('data/logs/RecruitInfoError.json', 'a', encoding='UTF-8') as file:
        json.dump(errorlog, file, indent=4, ensure_ascii=False)
        file.write('\n,')
    # insertDB("recruitInfoError", errorlog)


def getAllElement(driver, recruitInfoUrl):
    whereElement, tagElements, companyElement, detailElements, whereElement, deadlineElement, workAreaElement \
        = '', '', '', '', '', '', ''
    try:
        companyElement = driver.find_element_by_xpath('//section[@class="Bfoa2bzuGpxK9ieE1GxhW"]/div/h6/a')
    except Exception:
        saveError("elementError", recruitInfoUrl, 'warning: companyElement is null')
    try:
        detailElements = driver.find_elements_by_xpath('//section[@class="_1LnfhLPc7hiSZaaXxRv11H"]/p')
    except Exception:
        saveError("elementError", recruitInfoUrl, 'warning: detailElements is null')
    try:
        tagElements = driver.find_elements_by_xpath('//div[@class="ObubI7m2AFE5fxlR8Va9t"]/ul/li/a')
    except Exception:
        saveError("elementError", recruitInfoUrl, 'warning: tagElements is null')
    try:
        whereElement = driver.find_element_by_xpath(
            '/html/body/div[1]/div/div[3]/div[1]/div[1]/div/section[2]/div[1]/span')
    except Exception:
        saveError("elementError", recruitInfoUrl, 'warning: whereElement is null')
    try:
        workAreaElement = driver.find_element_by_xpath(
            '/html/body/div[1]/div/div[3]/div[1]/div[1]/div/div[2]/section[2]/div[2]/span[2]')
    except Exception:
        saveError("elementError", recruitInfoUrl, 'warning: workAreaElement is null')
    try:
        deadlineElement = driver.find_element_by_xpath(
            '/html/body/div[1]/div/div[3]/div[1]/div[1]/div[1]/div[2]/section[2]/div[1]/span[2]')
    except Exception:
        saveError("elementError", recruitInfoUrl, 'warning: deadlineElement is null')

    return [whereElement, tagElements, companyElement, detailElements, workAreaElement, deadlineElement]


def getInfosByElements(elements):
    # pattern = "[-=+,#/\?:$.@*\"※~&ㆍ!』\\|\(\)\[\]\<\>`\'…》{}‘’“”■❤♥(•️▶✔▪~및 \n^0-9]"
    pattern = '[^0-9a-zA-Zㄱ-힗%:.~\n]'
    region, _ = elements[0].text.split('\n.\n') if elements[0] else [None, None]
    tags = [re.sub(pattern=pattern, repl='', string=tagElement.text) for tagElement in elements[1]] if elements[
        1] else []
    company = elements[2].text if elements[2] else None
    workArea = elements[4].text if elements[4] else None
    deadline = elements[5].text if elements[5] else None

    details, detailsNouns = [], []
    if not elements[3]: details = []
    for detailElement in elements[3]:
        detail = re.sub(pattern=pattern, repl='', string=detailElement.text).strip()
        details.append(detail)

        englishTokens = nltk.word_tokenize(re.sub(f'[^a-zA-Z]', ' ', detailElement.text).strip())
        english = [token for token in englishTokens if token not in stopwordsEN]
        postprocessor = Postprocessor(twitter, passtags='Noun', stopwords=stopwordsKR, ngrams=ngrams)
        koreanWords = postprocessor.pos(detailElement.text)
        korean = [word[0] for word in koreanWords if len(word[0]) > 1 and word[0] != '앱']
        others = re.findall('[\d{10}]+[년|주|명|여명|시간|만원|원|인|개월]{1,5}', detailElement.text)

        temp = []
        temp.extend(korean)
        temp.extend(english)
        temp.extend(others)
        detailsNouns.append(temp)

    return region, tags, company, details, deadline, workArea, detailsNouns


def getRecruitInfo(recruitInfoUrl):
    # group = recruitInfoUrl['jobGroup']
    # url = recruitInfoUrl['url']
    pattern = '[^0-9\n]'
    group = recruitInfoUrl[0]
    url = recruitInfoUrl[1].strip()
    recruitInfoId =  re.sub(pattern=pattern, repl='', string=url)
    print(group, url)

    try:
        driver = connectWebDriver(url)
    except Exception as error:
        saveError("connectionError", recruitInfoUrl, error.args)
        return

    recruitInfoElements = getAllElement(driver, recruitInfoUrl)
    region, tags, company, details, deadline, workArea, detailsNouns = getInfosByElements(recruitInfoElements)

    contents = []
    contents.append(recruitInfoId)
    contents.append(group)
    contents.append(region)
    contents.append(company)
    contents.extend(details)
    contents.append(deadline)
    contents.append(workArea)

    # introduction = []
    # introduction.append(recruitInfoId)
    # introduction.extend(detailsNouns[0])
    # main_task = []
    # main_task.append(recruitInfoId)
    # main_task.extend(detailsNouns[1])
    # requirement = []
    # requirement.append(recruitInfoId)
    # requirement.extend(detailsNouns[2])
    # preference = []
    # preference.append(recruitInfoId)
    # preference.extend(detailsNouns[3])
    # benefit = []
    # benefit.append(recruitInfoId)
    # benefit.extend(detailsNouns[4])

    with open('data/recruitInfo/origin.csv', 'a', encoding='utf-8-sig', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(contents)
    with open('data/recruitInfo/tag.csv', 'a', encoding='utf-8-sig', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([recruitInfoId]+tags)
    # 회사소개
    with open('data/recruitInfo/introduction.csv', 'a', encoding='utf-8-sig', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([recruitInfoId]+detailsNouns[0])
    # 주요업무
    with open('data/recruitInfo/main_task.csv', 'a', encoding='utf-8-sig', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([recruitInfoId]+detailsNouns[1])
    # 자격요건
    with open('data/recruitInfo/requirement.csv', 'a', encoding='utf-8-sig', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([recruitInfoId]+detailsNouns[2])
    # 우대사항
    with open('data/recruitInfo/preference.csv', 'a', encoding='utf-8-sig', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([recruitInfoId]+detailsNouns[3])
    # 복지 및 혜택
    with open('data/recruitInfo/benefit.csv', 'a', encoding='utf-8-sig', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([recruitInfoId]+detailsNouns[4])
    print('완료!')
    driver.quit()


def scrapRecruitList(groups):
    origin_headers = ['직군', 'url']
    with open('data/recruitInfoList.csv', 'w', encoding='utf-8-sig', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(origin_headers)

    recruitInfosByGroup = manager.list()
    with closing(Pool(processes=5)) as pool:
        pool.starmap(getRecruitInfoList, zip(groups, repeat(recruitInfosByGroup)))

    print('직군별 채용공고리스트 url 저장 완료!')
    return recruitInfosByGroup


def openJsonFile(fileDir):
    with open(fileDir, 'w', encoding='UTF-8') as file:
        file.writelines('[\n')


def closeJsonFile(fileDir):
    with open(fileDir, 'a', encoding='UTF-8') as file:
        file.writelines([item for item in file[:-1]])
        file.write(']')


def scrapRecruitInfo(recruitInfoURLs):
    origin_headers = ['id', '직군', '지역', '회사명', '회사소개', '주요업무', '자격요건', '우대사항', '혜택 및 복지', '마감일', '근무지']
    with open('data/recruitInfo/origin.csv', 'w', encoding='utf-8-sig', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(origin_headers)
    with open('data/recruitInfo/tag.csv', 'w', encoding='utf-8-sig', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['id','태그'])
    with open('data/recruitInfo/introduction.csv', 'w', encoding='utf-8-sig', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['id','회사소개'])
    with open('data/recruitInfo/main_task.csv', 'w', encoding='utf-8-sig', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['id','주요업무'])
    with open('data/recruitInfo/requirement.csv', 'w', encoding='utf-8-sig', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['id','자격요건'])
    with open('data/recruitInfo/preference.csv', 'w', encoding='utf-8-sig', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['id','우대사항'])
    with open('data/recruitInfo/benefit.csv', 'w', encoding='utf-8-sig', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['id','혜택 및 복지'])

    # allRecruitInfo = manager.list()
    openJsonFile('data/logs/RecruitInfoError.json')
    with closing(Pool(processes=3)) as pool:
        pool.starmap(getRecruitInfo, zip(recruitInfoURLs))
        print('채용상세정보 수집 모두 완료!')

    # closeJsonFile('data/logs/RecruitInfoError.json')


if __name__ == '__main__':

    manager = Manager()
    # print('---------채용직군---------------------------')
    # jobGroups = getJobGroups()
    #
    # print('---------채용공고리스트----------------------')
    # recruitInfosByGroup = scrapRecruitList(jobGroups)

    print('---------채용공고---------------------------')
    # scrapRecruitInfo()

    # recruitInfosByGroup = readDB('recruitInfoUrl')
    with open('data/recruitInfoList.csv', 'r', encoding='utf-8-sig', newline='') as file:
        recruitInfosByGroup = [line.split(',') for line in file]
    # print(recruitInfosByGroup[1:])
    # recruitInfosByGroup = [['프론트엔드 개발자','https://www.wanted.co.kr/wd/42882']]
    scrapRecruitInfo(recruitInfosByGroup)
