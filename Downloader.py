import bridge as b
from bs4 import BeautifulSoup
import os

class Downloader:

    def __init__(self, sourse):
        self.area = b.area
        self.title_name = ""
        self.pieces = []
        self.from_file = sourse

    def download_azon(self):
        f = open(b.main_dir + '/' + self.from_file, 'r')
        data = f.read()
        self.title_name = BeautifulSoup(data, 'html.parser').find("title").text.strip()
        divs_outer = BeautifulSoup(data, 'html.parser').find_all("div", class_='styles_rowLight__3uy9z')
        for div in divs_outer:
            div_name = BeautifulSoup(str(div), 'html.parser').find("div", class_='styles_titleLight__1AL-E').text.strip()
            div_predata = BeautifulSoup(str(div), 'html.parser').find("div", class_='styles_valueLight__3Gl7S').text.strip()
            self.pieces.append((div_name, div_predata))
        f.close()

    def download_bookdepository(self):
        f = open(b.main_dir + '/' + self.from_file, 'r')
        data = f.read()
        self.title_name = BeautifulSoup(data, 'html.parser').find("title").text.strip()
        div = BeautifulSoup(data, 'html.parser').find("div", class_="biblio-info-wrap")
        h2 = BeautifulSoup(str(div), 'html.parser').find("h2", class_="biblio-title")
        ul = BeautifulSoup(str(div), 'html.parser').find("ul", class_="biblio-info")
        lis = BeautifulSoup(str(ul), 'html.parser').find_all("li")
        for li in lis:
            label = BeautifulSoup(str(li), 'html.parser').find("label").text.strip()
            span = BeautifulSoup(str(li), 'html.parser').find("span").text.strip()
            self.pieces.append((label, span))
        f.close()

    def run(self):
        if self.area == 'azon':
            self.download_azon()
        elif self.area == 'bookdepository':
            self.download_bookdepository()
