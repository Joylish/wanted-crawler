import json

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

def saveError(type, recruitInfoUrl, error):
    errorlog = {'type': type, 'where': recruitInfoUrl, 'detail': error}
    with open('data/logs/RecruitInfoError.json', 'a', encoding='UTF-8') as file:
        json.dump(errorlog, file, indent=4, ensure_ascii=False)
        file.write('\n,')
    # insertDB("recruitInfoError", errorlog)