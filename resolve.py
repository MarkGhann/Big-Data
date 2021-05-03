from mrjob.job import MRJob
from mrjob.job import MRStep
import bridge as b
import Downloader as d
import Translator as t
import sys
import os
import re
import json

class Resolver(MRJob):
    def drop_data_as_txt(self, file_name, name, neng, file1, file2):
        f = open(file_name, 'a', encoding='utf-8')
        f.write(name + "|" + neng + "\n")
        f.close()

    def easy_turn_data(self, lang, pieces):
        tr = t.Translator(b.tr_mapfile)
        lsecond_fixed = ""
        for i in pieces.split(' '):
            inm = tr.translate(i)
            inm = inm.replace('\n', '')
            inm = re.sub(r'\s+', ' ', inm)
            lsecond_fixed += inm + " "
        return lsecond_fixed, pieces

    def turn_data(self, lang, pieces):
        if lang == "RU":
            tr = t.Translator(b.tr_mapfile)
            simul = []
            for proop in pieces:
                lfirst = proop[0]
                lsecond = proop[1]
                lfirst = lfirst.replace('\n', '')
                lfirst = re.sub(r'\s+', ' ', lfirst)
                lsecond = lsecond.replace('\n', '')
                lsecond = re.sub(r'\s+', ' ', lsecond)
                lsecond_fixed = ""
                for i in lsecond.split(' '):
                    lsecond_fixed += tr.translate(i) + " "
                simul.append((lfirst, lsecond_fixed))
        return simul

    def mapper(self, _, line_path):
        read_file = open(b.main_dir + '/' + b.RTUD_DIR + b.THERE + b.BOOKDEPOSITORY_DIR + b.THERE + b.BOOKDEPOSITORY_DIR + ".json", "r")
        data = json.load(read_file)
        read_file.close()
        pieces = [i for i in data['Books'][int(line_path)]]
        name = data['Books'][int(line_path)]["Name"]
        ft = None
        fin = False
        if pieces:
            name, neng = self.easy_turn_data("RU", name)
            fi = open(b.main_dir + b.THERE + b.RTUD_DIR + b.THERE + b.AZON_AIM_FILE)
            for file_name in fi:
                file_name = file_name.replace('\n', '')
                read_file = open(b.main_dir + '/' + file_name, "r")
                cmps = json.load(read_file)
                d = int(cmps["Books"][-1]["Count"])
                for j in range(0, d):
                    cmp_name = cmps["Books"][j]["Name"].split('–')[0].split(':')[0]
                    read_file.close()
                    tr = t.Translator(b.tr_mapfile)
                    if name.replace(' ', '') == cmp_name.replace(' ', ''):
                        ft = file_name
                        fin = True
                        break
            fi.close()
            if fin:
                yield (name, neng), (line_path, ft)
            else:
                yield (name, neng), None
        else:
            yield (name, name), None
            
    def reducer(self, name, files):
        files = list(files)
        if files != [None] and files != None:
            self.drop_data_as_txt(b.deps_file, name[0], name[1], files[0][0], files[0][1])

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
        ]

b.main_dir = os.getcwd()
b.tr_mapfile = b.main_dir + '/' + 'word_map.txt'

b.deps_file = b.main_dir + "/" + "deps_file.txt"
f = open(b.deps_file, 'w', encoding='utf-8')
f.close()

resolver = Resolver()
resolver.run()
