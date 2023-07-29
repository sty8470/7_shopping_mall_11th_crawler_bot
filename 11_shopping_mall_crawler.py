# -*- conding: utf-8 -*-
'''
개요: 유저가 원하는 핵심 search_word을 11번가 웹사이트에서 추출하는 봇

실행방식: 유저가 검색을 원하는 핵심 search_word와 pageNum를 입력받는다.

[Workflow]
1. 유저에게 입력받은 search_word와 pageNum을 기준으로 openAPI request을 보낸다.
2. 정상적인 응답이 돌아오면, 핵심 데이터들을 정해진 디렉토리 안에 저장한다.
3. SQL에도 같이 저장한다.

'''

import pandas as pd
import logging
import requests
import json
import time
import traceback
import sys
import numpy as np
import os
import pandas as pd
import mysql.connector

from bs4 import BeautifulSoup
from datetime import datetime
from PyQt5.QtWidgets import *
from PyQt5.QtGui import *
from PyQt5.QtCore import *
from PyQt5 import uic


os.makedirs('./결과물', exist_ok=True)
os.makedirs('./상태로그', exist_ok=True)

download_folder = os.path.join(os.getcwd(), "결과물")


# 상태로그 파일이름 설정을 INFO 레벨로 지정하고 -> 로깅 파일 이름과 로깅 파일 형식을 지정한다.
logging.basicConfig(filename=f'상태로그/{datetime.today().strftime("%Y-%m-%d")}.log', level=logging.INFO, format='[%(asctime)s][%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
# PyQT5의 .ui 파일을 로드해서, WindowClass에 넘겨주고, setupUi 생성자로 layout 프레임을 넘겨준다.
ui_layout_class = uic.loadUiType("11_shopping_mall_crawler.ui")[0]

class WindowClass(QMainWindow, ui_layout_class):
    def __init__(self):
        # QMainWindow의 생성자를 호출하고, ui_layout_class의 UI 구성요소를 세팅합니다.
        super().__init__()
        self.setupUi(self)
        # 각 버튼들이 클릭되었을 때, 해당하는 메소드가 호출되도록 연결합니다.
        self.executeButton.clicked.connect(self.execute)
        self.stopButton.clicked.connect(self.stop)
        self.directButton.clicked.connect(self.direct)
        self.registerButton.clicked.connect(self.register)
        # 1초마다 start_working_thread의 함수를 호출하는 QTimer 객체를 생성합니다.
        self.timer = QTimer(self)
        self.timer.setInterval(1000)
        self.timer.timeout.connect(self.start_working_thread)
        # 현재 로그인 유무를 체크합니다.
        self.validated = False
    
    def execute(self):
        '''
        동작을 실행하기 전에 필요한 조건을 검사하고, 조건에 따라 알림 메시지를 표시하고 해당 동작을 실행하는 등의 작업을 수행합니다.
        '''
        if self.statusSignal.styleSheet() == 'color:green': 
            return

        if self.validated == False:
            msg = QMessageBox()
            msg.setWindowTitle("알림")
            msg.setText('검색어와 페이지 수 작성 필요')
            msg.setIcon(QMessageBox.Information)
            msg.exec_()
            return

        self.task_type = "동작"
        self.set_stylesheet("대기중")
        self.executeButton.setEnabled(False)
        self.stopButton.setEnabled(True)
        self.directButton.setEnabled(False)

        self.timer.start()

    def register(self):
        '''
        함수는 사용자가 제출한 로그인 정보를 서버에 전송하여 로그인을 시도하고, 
        응답에 따라 로그인 성공 또는 실패를 처리합니다. 로그인 성공 시 사용자 정보를 저장하고 필드를 비활성화하는 등의 작업을 수행합니다.
        '''

        self.search_word = self.search_word_line_edit.text()
        self.page_num = self.page_num_line_edit.text()

        self.search_word_line_edit.clear()
        self.page_num_line_edit.clear()

        self.search_word_line_edit.setDisabled(True)
        self.page_num_line_edit.setDisabled(True)
        self.stopButton.setDisabled(True)

        if self.search_word.replace(' ','').strip().isalpha() and self.page_num.replace(' ','').strip().isdigit() and len(self.search_word) > 0 and len(self.page_num) > 0:
            self.validated = True
            msg = QMessageBox()
            msg.setWindowTitle("알림")
            msg.setText('검색어와 페이지 수 등록 성공')
            self.registerButton.setDisabled(True)
            msg.setIcon(QMessageBox.Information)
            msg.exec_()
        else:
            msg = QMessageBox()
            msg.setWindowTitle("알림")
            msg.setText('검색어와 페이지 수 등록 실패')
            msg.setIcon(QMessageBox.Information)
            msg.exec_()

    def direct(self):
        if self.statusSignal.styleSheet() == 'color:blue': 
            return

        if self.validated == False:
            msg = QMessageBox()
            msg.setWindowTitle("알림")
            msg.setText('검색어와 페이지 수 작성 필요')
            msg.setIcon(QMessageBox.Information)
            msg.exec_()
            return
        
        self.task_type = "즉시실행"
        self.set_stylesheet("대기중")
        self.executeButton.setEnabled(False)
        self.directButton.setEnabled(False)

        self.main_thread = ShoppingCrawler(self,self.search_word,self.page_num)
        self.main_thread.log.connect(self.set_log)
        self.main_thread.finished.connect(self.working_finished)
        self.main_thread.start()

        self.set_log('Started!')
    
    def stop(self):
        self.set_stylesheet("미동작")
        self.timer.stop()
        self.executeButton.setEnabled(True)
        self.stopButton.setEnabled(False)
        self.directButton.setEnabled(True)

    def working_finished(self):
        # 객체의 실행이 완료될 때 까지 기다립니다.
        self.main_thread.wait()
        # 이 메서드는 객체의 소유권을 Qt 이벤트 루프에게 양도하고, 안전하게 객체를 삭제하기 위해 이벤트 큐에 삭제 이벤트를 추가합니다.
        self.main_thread.deleteLater()
        # self.main_thread 변수를 삭제합니다. 이렇게 하면 메모리에서 해당 객체에 대한 참조가 제거됩니다.
        del self.main_thread

        if self.task_type == "동작":
            self.set_stylesheet("대기중")
        elif self.task_type == "즉시실행":
            self.set_stylesheet("미동작")
            self.executeButton.setEnabled(True)
            self.directButton.setEnabled(True)

    def set_stylesheet(self,flag):
        '''
        UserRewardsBot 클래스의 작업읭 성격에 따라서, 상태색깔과 상태메세지를 업데이트 합니다.
        '''
        if flag == "대기중": 
            self.statusSignal.setStyleSheet('color:green')
            self.boardLabel.setText("대기중")
        elif flag == "동작중": 
            self.statusSignal.setStyleSheet('color:blue')
            self.boardLabel.setText("동작중")
        elif flag == "미동작": 
            self.statusSignal.setStyleSheet('color:red')
            self.boardLabel.setText("미동작")

    def start_working_thread(self):
        '''
        UserRewardsBot 클래스의 작업이 시작될 때 동작하는 구간
        '''
        # 현재 시간을 할당하기
        time = QTime.currentTime()
        # time_arr 메소드를 호출합니다 -> 현재 self.arr 리스트에 추가되어 있는 "Hour" 체크하기
        self.time_arr()
        # self.arr에 추가되어있는 정각시간이 되면,
        if time.toString('mm.ss') == '00.00' and time.toString('hh') in self.arr:
            # main_thread을 호출하여서 당시의 INFO 로그를 남기고 쓰레드 실행/종료를 시작합니다.
            self.main_thread = ShoppingCrawler(self,self.search_word,self.page_num)
            self.main_thread.log.connect(self.set_log)
            self.main_thread.finished.connect(self.working_finished)
            self.main_thread.start()
            self.set_log('Started!')

    def time_arr(self):
        '''
        이 함수가 호출되면, 00시부터 23시까지 해당 시간(시간문자열)을 self.arr 리스트에 추가합니다.
        이는 배치성 작업으로 인해서 1시간 마다 self.arr 리스트에 추가된 시간에 해당 봇이 동작하기 위해서 관리하기 위함입니다.
        '''
        self.arr = []
        if self.time00Hour.isChecked(): self.arr.append('00')
        if self.time01Hour.isChecked(): self.arr.append('01')
        if self.time02Hour.isChecked(): self.arr.append('02')
        if self.time03Hour.isChecked(): self.arr.append('03')
        if self.time04Hour.isChecked(): self.arr.append('04')
        if self.time05Hour.isChecked(): self.arr.append('05')
        if self.time06Hour.isChecked(): self.arr.append('06')
        if self.time07Hour.isChecked(): self.arr.append('07')
        if self.time08Hour.isChecked(): self.arr.append('08')
        if self.time09Hour.isChecked(): self.arr.append('09')
        if self.time10Hour.isChecked(): self.arr.append('10')
        if self.time11Hour.isChecked(): self.arr.append('11')
        if self.time12Hour.isChecked(): self.arr.append('12')
        if self.time13Hour.isChecked(): self.arr.append('13')
        if self.time14Hour.isChecked(): self.arr.append('14')
        if self.time15Hour.isChecked(): self.arr.append('15')
        if self.time16Hour.isChecked(): self.arr.append('16')
        if self.time17Hour.isChecked(): self.arr.append('17')
        if self.time18Hour.isChecked(): self.arr.append('18')
        if self.time19Hour.isChecked(): self.arr.append('19')
        if self.time20Hour.isChecked(): self.arr.append('20')
        if self.time21Hour.isChecked(): self.arr.append('21')
        if self.time22Hour.isChecked(): self.arr.append('22')
        if self.time23Hour.isChecked(): self.arr.append('23')

    def set_log(self,data):
        '''
        ListWidget에 실행시 로그를 남겨서 업데이트 하며, 이 정보는 "상태로그" 밑에 저장됩니다.
        '''
        self.listWidget.insertItem(0,f"{datetime.today().strftime('[%Y-%m-%d %H:%M:%S]')} {str(data)}")
        logging.info(str(data))
        

class ShoppingCrawler(QThread):
    log = pyqtSignal(str)

    def __init__(self, parent, search_word, page_num):
        super().__init__(parent)
        self.search_word = search_word
        self.page_num = page_num
        self.all_data = []

    def get_search_query_response(self):
        url = "http://openapi.11st.co.kr/openapi/OpenApiService.tmall"

        for i in range(1, int(self.page_num) + 1):
            params = {
                "key": "c99d87cf912fddc49250550426b3d7c8",
                "apiCode": "ProductSearch",
                "keyword": self.search_word,
                "pageSize": 100,
                "pageNum": i,
            }

            response = requests.get(url, params=params)
            if response.status_code != 200:
                self.log.emit("해당 제품 API콜에 문제가 생겼습니다!")
                logging.error("API 콜 에러 발생")
                return
            else:
                try:
                    xmlData = response.content.decode('cp949')
                    soup = BeautifulSoup(xmlData, "html.parser")

                    self.product_name = [i.text for i in soup.find_all('productname')]
                    self.product_price = [j.text for j in soup.find_all('productprice')]

                    data = {'Product Name': self.product_name, 'Product Price': self.product_price}
                    self.all_data.append(pd.DataFrame(data))

                except:
                    self.log.emit("해당 제품 파싱 실패!")
                    logging.error("해당 제품 파싱 실패!")
        
    def save_data_to_csv(self):

        # Concatenate data from all pages into a single DataFrame
        df = pd.concat(self.all_data)
        df = df.reset_index(drop=True)
        df.index += 1

        try:
            # DataFrame을 CSV 파일로 저장 (덮어쓰기)
            file_path = os.path.join(download_folder, 'product_data.csv')
            df.to_csv(file_path, index=True, encoding='utf-8-sig')
        except:
            self.log.emit("DataFrame을 CSV로 저장 실패!")
            logging.error("DataFrame을 CSV로 저장 실패!")
            
    def insert_data_to_mysql(self):

        # MySQL 서버 연결 설정
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='1234',
            database='11_shop'  # 데이터를 저장할 데이터베이스 이름
        )

        # 데이터 삽입 SQL 쿼리
        insert_query = "INSERT INTO SonUniform (product_name, product_price) VALUES (%s, %s)"

        # SQL 쿼리 실행을 위한 커서 생성
        cursor = connection.cursor()

        try:
            # 데이터를 한 줄씩 삽입
            for i in range(len(self.product_name)):
                data = (self.product_name[i], self.product_price[i])
                cursor.execute(insert_query, data)

            # 변경사항 커밋
            connection.commit()
            logging.info("데이터가 성공적으로 MySQL에 저장되었습니다!")
            self.log.emit("데이터가 성공적으로 MySQL에 저장되었습니다!")

        except mysql.connector.Error as error:
            # 에러가 발생한 경우 변경사항 롤백
            connection.rollback()
            error_message = "MySQL 에러: " + str(error)
            logging.error(error_message)
            self.log.emit("MySQL 에러: " + str(error))

        finally:
            # 커서와 연결 닫기
            cursor.close()
            connection.close()
        
    def run(self):
        try:
            self.get_search_query_response()
            self.save_data_to_csv()
            self.insert_data_to_mysql()
            self.log.emit("모든 크롤링이 끝났습니다.")
        
        except:
            error = traceback.format_exc()
            logging.error(error)
            logging.info(error)
            self.log.emit(error)
        
        
if __name__ == "__main__" :

    app = QApplication(sys.argv) 
    myWindow = WindowClass() 
    myWindow.show()
    app.exec_()