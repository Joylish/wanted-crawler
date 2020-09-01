import requests
from bs4 import BeautifulSoup
from webConnection import connectWebDriver, scrollPage

from contextlib import closing
from multiprocessing import Pool, Manager
from itertools import repeat

import re
from fileIO import openJsonFile, closeJsonFile, saveError
from dbIO import readDB, insertDB

import nltk
from nltk.corpus import stopwords
from konlpy.tag import Okt
from ckonlpy.tag import Twitter, Postprocessor
from ckonlpy.utils import load_wordset, load_ngram

# nltk.download('punkt')
# nltk.download('stopwords')
okt = Okt()
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
    insertDB("jobGroup", jobGroups)
    return jobGroups



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
            # with open('data/recruitInfoList.csv', 'a', encoding='utf-8-sig', newline='') as file:
            #     writer = csv.writer(file)
            #     writer.writerow([jobGroup, recruitInfoUrl])
    driver.quit()


def scrapRecruitList(groups):
    # origin_headers = ['직군', 'url']
    # with open('data/recruitInfoList.csv', 'w', encoding='utf-8-sig', newline='') as file:
    #     writer = csv.writer(file)
    #     writer.writerow(origin_headers)

    recruitInfosByGroup = manager.list()
    with closing(Pool(processes=5)) as pool:
        pool.starmap(getRecruitInfoList, zip(groups, repeat(recruitInfosByGroup)))
    insertDB("recruitInfos", recruitInfosByGroup)
    print('직군별 채용공고리스트 url 저장 완료!')
    return recruitInfosByGroup


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
    tagPattern = '[^0-9a-zA-Zㄱ-힗%:.~\n]'
    detailPattern = '[^0-9a-zA-Zㄱ-힗%:.~ #+\n]'
    region, _ = elements[0].text.split('\n.\n') if elements[0] else [None, None]
    tags = [re.sub(pattern=tagPattern, repl='', string=tagElement.text) for tagElement in elements[1]] if elements[
        1] else []
    company = elements[2].text if elements[2] else None
    workArea = elements[4].text if elements[4] else None
    deadline = elements[5].text if elements[5] else None

    details, detailsNouns = [], []
    if not elements[3]: details = []
    for detailElement in elements[3]:
        detail = re.sub(pattern=detailPattern, repl='', string=detailElement.text).strip()
        details.append(detail)

        englishTokens = nltk.word_tokenize(re.sub(f'[^a-zA-Z]', ' ', detailElement.text).strip())
        english = [token.lower() for token in englishTokens if token not in stopwordsEN]
        postprocessor = Postprocessor(twitter, passtags='Noun', ngrams=ngrams, stopwords=stopwordsKR)
        koreanWords = postprocessor.pos(detail)
        # koreanWords = okt.nouns(detail)
        print(koreanWords)
        korean = [word for word, _ in koreanWords if len(word) > 1 and word != '앱']
        others = re.findall('[\d{2}]년]', detailElement.text)
        temp = []
        temp.extend(korean)
        temp.extend(english)
        temp.extend(others)
        detailsNouns.append(temp)
    print(detailsNouns)

    return region, tags, company, details, deadline, workArea, detailsNouns

def createrecruitInfo(contents):
    # headers = ['id', '직군', '지역', '국가', '태그', '회사명', '회사소개', '주요업무', '자격요건', '우대사항', '혜택 및 복지', '마감일', '근무지']
    headers = ['직군', '지역', '태그', '회사명', '회사소개', '주요업무', '자격요건', '우대사항', '혜택 및 복지', '마감일', '근무지', '회사소개_명사', '주요업무_명사', '자격요건_명사', '우대사항_명사', '혜택 및 복지_명사']
    recruitInfo = {header: value for header, value in zip(headers, contents)}
    # with open('data/RecruitInfoLog.json', 'a', encoding='UTF-8') as file:
    #     json.dump(recruitInfo, file, indent=4, ensure_ascii=False)
    #     file.write('\n,')
    return recruitInfo

def getRecruitInfo(recruitInfoUrl, allRecruitInfo):
    print(recruitInfoUrl)

    pattern = '[^0-9]'
    group = recruitInfoUrl['jobGroup']
    url = recruitInfoUrl['url']
    # group = recruitInfoUrl[0]
    # url = recruitInfoUrl[1].strip()
    recruitInfoId = re.sub(pattern=pattern, repl='', string=url)

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
    contents.extend(detailsNouns)

    recruitInfo = createrecruitInfo(contents)
    allRecruitInfo.append(recruitInfo)

    insertDB("recruitInfo", recruitInfo)
    # REQUIRED = [recruitInfoId,  region, company, workArea]
    # with open('data/recruitInfo/origin.csv', 'a', encoding='utf-8-sig', newline='') as file:
    #     writer = csv.writer(file)
    #     writer.writerow(contents)
    # with open('data/recruitInfo/tag.csv', 'a', encoding='utf-8-sig', newline='') as file:
    #     writer = csv.writer(file)
    #     writer.writerow(REQUIRED+tags)
    # # 회사소개
    # with open('data/recruitInfo/introduction.csv', 'a', encoding='utf-8-sig', newline='') as file:
    #     writer = csv.writer(file)
    #     writer.writerow(REQUIRED+detailsNouns[0])
    # # 주요업무
    # with open('data/recruitInfo/main_task.csv', 'a', encoding='utf-8-sig', newline='') as file:
    #     writer = csv.writer(file)
    #     writer.writerow(REQUIRED+detailsNouns[1])
    # # 자격요건
    # with open('data/recruitInfo/requirement.csv', 'a', encoding='utf-8-sig', newline='') as file:
    #     writer = csv.writer(file)
    #     writer.writerow([recruitInfoId]+detailsNouns[2])
    # # 우대사항
    # with open('data/recruitInfo/preference.csv', 'a', encoding='utf-8-sig', newline='') as file:
    #     writer = csv.writer(file)
    #     writer.writerow(REQUIRED+detailsNouns[3])
    # # 복지 및 혜택
    # with open('data/recruitInfo/benefit.csv', 'a', encoding='utf-8-sig', newline='') as file:
    #     writer = csv.writer(file)
    #     writer.writerow(REQUIRED+detailsNouns[4])
    print('완료!')
    driver.quit()



def scrapRecruitInfo(recruitInfoURLs):
    # origin_headers = ['id', '직군', '지역', '회사명', '회사소개', '주요업무', '자격요건', '우대사항', '혜택 및 복지', '마감일', '근무지']
    # with open('data/recruitInfo/origin.csv', 'w', encoding='utf-8-sig', newline='') as file:
    #     writer = csv.writer(file)
    #     writer.writerow(origin_headers)
    # with open('data/recruitInfo/tag.csv', 'w', encoding='utf-8-sig', newline='') as file:
    #     writer = csv.writer(file)
    #     writer.writerow(['id','태그'])
    # with open('data/recruitInfo/introduction.csv', 'w', encoding='utf-8-sig', newline='') as file:
    #     writer = csv.writer(file)
    #     writer.writerow(['id','회사소개'])
    # with open('data/recruitInfo/main_task.csv', 'w', encoding='utf-8-sig', newline='') as file:
    #     writer = csv.writer(file)
    #     writer.writerow(['id','주요업무'])
    # with open('data/recruitInfo/requirement.csv', 'w', encoding='utf-8-sig', newline='') as file:
    #     writer = csv.writer(file)
    #     writer.writerow(['id','자격요건'])
    # with open('data/recruitInfo/preference.csv', 'w', encoding='utf-8-sig', newline='') as file:
    #     writer = csv.writer(file)
    #     writer.writerow(['id','우대사항'])
    # with open('data/recruitInfo/benefit.csv', 'w', encoding='utf-8-sig', newline='') as file:
    #     writer = csv.writer(file)
    #     writer.writerow(['id','혜택 및 복지'])

    allRecruitInfo = manager.list()
    openJsonFile('data/logs/RecruitInfoError.json')
    with closing(Pool(processes=5)) as pool:
        pool.starmap(getRecruitInfo, zip(recruitInfoURLs, repeat(allRecruitInfo)))
    print('채용상세정보 수집 모두 완료!')

    closeJsonFile('data/logs/RecruitInfoError.json')


if __name__ == '__main__':

    manager = Manager()
    # print('---------채용직군---------------------------')
    # jobGroups = getJobGroups()
    #
    # print('---------채용공고리스트----------------------')
    # recruitInfosByGroup = scrapRecruitList(jobGroups)
    #
    # print('---------채용공고---------------------------')
    # scrapRecruitInfo()

    recruitInfosByGroup = readDB('recruitInfos')
    # with open('data/recruitInfoList.csv', 'r', encoding='utf-8-sig', newline='') as file:
    #     recruitInfosByGroup = [line.split(',') for line in file]
    # print(recruitInfosByGroup)
    # recruitInfosByGroup = [['프론트엔드 개발자','https://www.wanted.co.kr/wd/42882']]
    # recruitInfosByGroup =[{'jobGroup': '프론트엔드 개발자', 'url': 'https://www.wanted.co.kr/wd/43046'}]
    scrapRecruitInfo(recruitInfosByGroup)
