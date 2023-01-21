#OPEX 파일 DB 투입기
#v5. 230121
#v5 :
#메모리가 터졌으니.. DB를 먼저 쌓고 # ok
#파일 1개씩 열어
#시트별로 DB에 올리고 동시에 메모리는 반환

# 0. 선언부
import pandas as pd
import os
import glob
import time

# 전역변수부

# I. 파일 리스트 인식
def rec():
    #실행할 위치를 지정
    workPath = "C:/test_mysql"
    os.chdir(workPath)
    location = os.getcwd()

    #순환하면서 폴더내의 *xlsb를 리스트에 넣음
    excels = glob.glob('*.xlsb*')

    #len(excels)    

    #전역변수 exl_list    
    exl_list = []
    for excel in excels:
        #print(excel)
        if excel[0] == "~": #임시파일 무시
            print(excel[0],"은 임시파일이므로 등록하지 않습니다.")
            continue
        exl_list.append(excel)

    #print(exl_list) #12개의 파일이 exl_list에 남음
    print(len(exl_list), "개의 파일에 대해 반복 순환합니다.")
    return exl_list

# II. 시트 순환부
def cirSheet(fileName, tableName):
    global programBeginTime
    print(fileName,"파일 작업개시.")
    # 일단 첫번째 파일만
    #인수 fileName은 작업대상 파일명. 여러 시트로 구성되어 있으므로 다시 순환 필요

    
    #판다스 엑셀파일 객체로 지정
    workFile = pd.ExcelFile(fileName)

    sheetNo = len(workFile.sheet_names) #시트명들의 갯수 : sheetNo
    print(sheetNo, "개의 시트에 대해 반복 순환합니다.")

    #while=True로 일단 시작하면서 i를 1씩 늘림
    #에러나면 구문 종료

    i=0 #지역변수 초기화
    accResult =0 #누적갯수

    while(True):

        if i==sheetNo: #0부터 시작하니 같으면 종료
            print(fileName,"파일의 시트순환을 끝냈습니다. ",accResult,"갯수의 Record를 해당 파일에서 Insert하였습니다.")
            #dfFn.columns = dfColumns #다시 헤더 지정. 
            return accResult

        #0부터 순환돌면서 시트 반복
        print(i+1,"번째 순환대상 시트:",workFile.sheet_names[i]) #논리명
        sheetBeginTime = time.time()

        if (workFile.sheet_names[i][:5] != "Sheet"): #만약 시트이름이 Sheet로 시작 안한다면 (즉 더미시트면)
            print(workFile.sheet_names[i],"는 더미시트입니다. 작업을 생략합니다.")
            i+=1
            continue

        #데이터프레임 추출부 : 최초냐에 따라 분기
        #첫번째 순환이라면 (i=0)        
        if i==0:
            df = pd.read_excel(fileName, i) #header = Not None. 왜냐면 첫 시트는 헤더가 들어 있음
            tmpColumns = df.columns = df.columns.str.strip() #str.strip() 호출해서 trim #첫번째에만 수행한다. 왜냐하면 trim 안된 헤더가 붙어 있으니까.
        else:
            df = pd.read_excel(fileName, i, header=None)
            #df.columns = tmpColumns #최초에 따로 저장해놓은 헤더를 달아준다.
            df.columns = tmpColumns[:len(df.columns)] #그 중에 해당하는 부분만큼만..            
        
        #공통수행부
        
        print(df.shape[0],"개의 Record가 있는 시트입니다.")
        print(i+1,"번째 시트 Read 작업시간:",round(time.time()-sheetBeginTime,0),"초/ 누적 작업시간:",round(time.time()-programBeginTime,0),"초")   
        result = insert(df, tableName) #인수로 받은 tableName을 바로 다시 인수로 넣음
        #print(i+1,"번째 시트 작업시간:",round(time.time()-sheetBeginTime,0),"초")   
        
        accResult += result
        i+=1

#III. DB Insert부
import pymysql
from sqlalchemy import create_engine
pymysql.install_as_MySQLdb()
import MySQLdb

def insert(df, tableName):

    #DB연결부
    mySQL_ID = "root"
    mySQL_PW = "genius"
    mySQL_DB = "testdb"

    engine = create_engine("mysql+mysqldb://"+mySQL_ID+":"+mySQL_PW+"@127.0.0.1/"+mySQL_DB, encoding='utf-8')
    conn=engine.connect()
    print("DB에 연결되었습니다.")

    #Insert부
    global programBeginTime
    insertBeginTime = time.time()

    df.columns = df.columns.str.strip() #to_SQL을 위해 공백 제거 (중복이라 안해도 될듯)
    result = df.to_sql(name=tableName, con=engine, index=False, if_exists="append") #index 제거, append로 해야 계속 넣을 수 있음
    print("시트 DB입력이 완료되었으며,"," 시트의 Record 갯수는 ",result,"개입니다.") 
    print("Insert 작업시간:",round(time.time()-insertBeginTime,0),"초/ 누적 작업시간:",round(time.time()-programBeginTime,0),"초")   
    return result


#파일순환부
def cirFile(exl_list):    
    tableName = input(">>테이블 명을 입력하시오. :")    
    i=0 #파일순환갯수변수
    accResultTotal = 0 #Insert누적갯수변수
    #global dfFnF

    #programBeginTime = time.time()
    global programBeginTime

    #"파일"순환부
    for j in exl_list: #exl_list는 앞서 반환받은 작업대상 파일명 리스트        
        print(i+1,"번째 파일 작업개시") #+1로 논리명으로
        fileBeginTime = time.time()
        accResult = cirSheet(j, tableName) #작업대상 파일명을 매개변수 j / tableName : 테이블명
        accResultTotal += accResult
        print("누적 작업 Record 갯수는 ",accResultTotal)        
        print(i+1,"번째 파일 작업시간:",round(time.time()-fileBeginTime,0),"초/ 누적 작업시간:",round(time.time()-programBeginTime,0),"초")        
        
        i+=1

        # Test Code
        # if i==2: #0,1 2개만 실시해보자
        #     break #for문 탈출
        # Test Code            
    
    print("총 작업시간:",round(time.time()-programBeginTime,0))
    return accResultTotal

#IV. 실행부
if(__name__=="__main__"):      

    #전역변수
    programBeginTime = time.time()

    print("OPEX 작업개시...")
    exl_list = rec() #대상을 인식하는 메서드
    accResultTotal = cirFile(exl_list) #파일을 순환하는 메서드
    print("OPEX 작업완료...")
    print("총 Inserted Record 갯수는:",accResultTotal)