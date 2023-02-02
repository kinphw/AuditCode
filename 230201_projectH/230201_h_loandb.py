#####
#230201. H사 LoanDB 샘플림
#####

# 라이브러리 선언부
import os
import pandas as pd
import openpyxl
import time

##################

def doit():

#os.chdir("C:/projects/test_230201")

###폴더 안의 txt파일을 추가
    filelist = os.listdir()
    file_list_txt = [file for file in filelist if file.endswith(".txt")]

    ###dic에 데이터프레임 추가
    dic = {}
    for file in file_list_txt:
        dic[file] = pd.read_csv(file, encoding="cp949", delim_whitespace=True)
        print(file," 완료")

    #len(dic)


    ###dicName에 이름 추가
    i=0
    dicName = {}
    for file in file_list_txt:    
        dicName[i] = file
        i= i+1
    print(dicName, "추출함")

    ###column명을 엑셀로 추출

    wb = openpyxl.Workbook() #객체 생성
    ws = wb['Sheet'] #초기 시트명

    lst = []

    for i in range(0, len(dic)):    
        tmp = list(dic.get(dicName.get(i)).columns)
        lst.append(tmp)
        ws.append(lst[i])

    xlsFileName = "Column Name.xlsx"

    wb.save(xlsFileName)
    print(xlsFileName,"으로 컬럼리스트를 추출하였음")

    ###dic1 복제 (dic 재사용)
    dic1 = dic.copy()

    ###dic1에서 대상외 삭제
    dic1.pop(dicName.get(3))
    dic1.pop(dicName.get(7))
    dic1.pop(dicName.get(8))

    #len(dic1) #6이어야 함

    ###dic1의 5번째 데이터프레임 colmumns 변경
    #dic1.get(dicName.get(5)).columns

    tmp = dic1.get(dicName.get(5)).rename(columns={"손상여부":"부도여부", "PDSEG":"POOL_ID", "SUM(대출잔액)":"SUM(미수잔액)", "SUM(I9_충당금_UEA)":"SUM(I9_UEA)"})
    dic1[dicName.get(5)] = tmp
    print("요건대로 컬럼명을 변경함")

    #len(dic1)

    ###tgtDfs로 추출
    tgtDfs = dic.values()
    tgtDfs = list(tgtDfs)
    #type(tgtDfs)
    #len(tgtDfs)

    ###합치기
    concatDfs = pd.concat(tgtDfs,ignore_index=False, join='inner')
    print("난내 통합 LoanDB 추출 완료")

    ###행수 검증
    i = 0
    for t in tgtDfs:
        i=i+t.shape[0]

    print(i, "개의 Records가 있습니다.")

    concatDfs.shape

    ###이 단계에서 concatDfs 는 난내총파일임

    ###일단 복제

    concatDfs1 = concatDfs.copy()

    ###STAGE_구분코드 내역 보기
    #concatDfs1.columns
    #tmp = concatDfs1["STAGE_구분코드"].drop_duplicates()
    #1,2,3

    ###groupby 해서 별도 DF로 추출
    groupedDf = concatDfs1.groupby("STAGE_구분코드")

    stage1Df = groupedDf.get_group(1)
    stage2Df = groupedDf.get_group(2)
    stage3Df = groupedDf.get_group(3)
    print("Stage별 그룹으로 추출하였습니다.")

    ###30개씩 샘플링
    sampleStage1 = stage1Df.sample(30, random_state = 2023)
    sampleStage2 = stage2Df.sample(30, random_state = 2023)
    sampleStage3 = stage3Df.sample(30, random_state = 2023)
    print("Stage별 30개씩 난수추출하였습니다.")

    ###합치기. 90개가 되야함
    sampleTotal = pd.concat([sampleStage1,sampleStage2,sampleStage3], ignore_index=False, join="inner")

    #sampleTotal #30개씩 무작위샘플링

    #리스크 통합 Df 생성

    #다시 dic1에 데이터프레임 추가
    dic1 = dic #복제해둔 것 재사용
    # for file in file_list_py:
    #     dic1[file] = pd.read_csv(file, encoding="cp949", delim_whitespace=True)
    #     print(file,"완료")

    ###리스크 2개 추출
    #len(dic1)
    #dicName
    riskDf1 = dic1.get(dicName.get(7))
    riskDf2 = dic1.get(dicName.get(8))

    ###합치기.
    tmp = riskDf1.shape[0] + riskDf2.shape[0]
    print(tmp, "행의 리스크파일을 추출하였습니다. (Lookup 목적)") #TOBE 갯수

    risksumDf = pd.concat([riskDf1, riskDf2], ignore_index=False, join="inner") #리스크 합산 데이터프레임 완성

    #risksumDf.shape #검증

    ###혹시 모르니 복제해 둠
    sampleTotal1 = sampleTotal.copy() #Deep copy

    ###90건 샘플에 risksumDf를 이너조인해서 붙임
    sampleTotalJoin = sampleTotal1.merge(risksumDf,left_on="발급회원번호", right_on="발급회원번호")

    # sampleTotalJoin
    # sampleTotalJoin["STAGE_구분코드_x"]
    # sampleTotalJoin.shape
    try:
        tmpTxt = "샘플링 90건에 리스크데이터 조인.xlsx"
        sampleTotalJoin.to_excel(tmpTxt, index=None)
        print("산출물1을 Excel로 추출 :",tmpTxt)
    except:
        print("오류. 동일한 파일이 사용중입니다.")

    # sampleTotal1.shape
    # risksumDf.shape

    ###산출물22 : 난내대출 인덱스 추출
    #concatDfs.head()

    #concatDfs.columns

    forMUS = concatDfs[["BS계정과목코드", "SUM(미수잔액)","SUM(미수이자)"]]

    ###엑셀로 저장
    tmpTxt = "MUS샘플링용 난내대출 population.xlsx"
    try:
        forMUS.to_csv(tmpTxt)
    except:
        print("Error. 파일 사용중")
    else:
        print("산출물2 추출 : ", tmpTxt)


# forMUS.shape

###결과물 저장
#1. 난내대출 모집단
# concatDfs.to_pickle('./concatDfs.pickle')

#2. 리스크 합산파일
# risksumDf.to_pickle('./risksumDf.pickle')

#3. 스테이지별 그루핑
# stage1Df.to_pickle('./stage1Df.pickle')
# stage2Df.to_pickle('./stage2Df.pickle')
# stage3Df.to_pickle('./stage3Df.pickle')

if(__name__=="__main__"):
    print("######")
    print("project H LoanDB 샘플링")
    print("######")
    startTime = time.time()
    doit()
    totalTime = time.time()-startTime
    print("작업완료.", totalTime,"초 소요")

#인덱스+계정과목코드+sum(미수잔액)+sum(미수이자)로 부탁드려도 될까요?


#네네 론디비에서 한도는 제외하고 나머지를 공통되는 열만 합쳐서 > 난내 총파일 만들고
#난내에서 stage 1 , 2, 3 각 30개씩 뽑은 다음에
#90개 뽑힌 것에 대해서 발급회원번호를 key로 해서 리스크에 있는 정보를 붙이려고 합니다

#리스크1, 2도 > 리스크 총파일로 만들어야 겠네요..
#그리고 난내 총파일에서 대출잔액에 대해서도 샘플링이 필요해요..
#그건 담당자가 다른분이긴 하지만,, 같이 전달드리려 합니다ㅠㅠ

#stage는 attribute sample이라서 금액상관없이 random 으로 뽑으려 하구요.. 대출잔액은 MUS 이용하려고 합니당