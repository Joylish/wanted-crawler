import requests
from bs4 import BeautifulSoup
from selenium import webdriver
import time,csv
# 이후
from multiprocessing import Pool, Manager

# 직업군 불러오기
def getJobGroups():
    res = requests.get('https://www.wanted.co.kr/wdlist/518?country=kr&job_sort=job.latest_order&years=-1&locations=all')
    html = res.text
    soup = BeautifulSoup(html, "html.parser")

    jobGroups=[]
    for elements in soup.find("div", class_="_2h5Qtv_8mK2LOH-yR3FTRs").find_all("li"):
        href = elements.find("a")["href"]
        jobGroups.append("https://www.wanted.co.kr"+href)
        JobGroupfile.writerow([href])
        print(href)
    return jobGroups

def getRecruitInfos(url):
    # 본인 컴퓨터에서 chromedriver가 있는 위치 넣기
    driver = webdriver.Chrome('chromedriver/chromedriver')
    driver.implicitly_wait(3)
    driver.get(url)
    driver.implicitly_wait(3)

    SCROLL_PAUSE_TIME = 2

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

    allRecruitInfo = driver.find_elements_by_xpath('//div[@id="_3D4OeuZHyGXN7wwibRM5BJ"]/a')
    print(recruitInfos)
    print('-------------------------')
    for recruitInfo in allRecruitInfo:
        recruitInfos.append(recruitInfo)
        RecruitListfile.writerow([recruitInfo])
        print(recruitInfo)
    driver.quit()

if __name__=='__main__':
    file1 = open('data/JobGroup.csv', 'w', encoding='utf-8', newline='')
    file2 = open('data/RecruitList.csv', 'w', encoding='utf-8', newline='')
    file3 = open('data/RecruitDetail.csv', 'w', encoding='utf-8', newline='')

    JobGroupfile = csv.writer(file1)
    RecruitListfile = csv.writer(file2)
    RecruitDetailfile = csv.writer(file3)

    JobGroupfile.writerow(['JOB_GROUP_URL'])
    RecruitListfile.writerow(['RECRUIT_URL'])
    RecruitDetailfile.writerow(['JOB_GROUP', 'COMPANY', 'REGION', 'CONTENTS'])

    pool = Pool(processes=4)
    manager = Manager()

    jobGroups = getJobGroups()
    recruitInfos = manager.list()
    pool.map(getRecruitInfos, jobGroups)

    JobGroupfile.close()
    RecruitListfile.close()
    RecruitDetailfile.close()