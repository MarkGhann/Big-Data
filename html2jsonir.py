from mrjob.job import MRJob
from mrjob.job import MRStep
import bridge as b
import Downloader as d
import Translator as t
import sys
import os
import re


def set_azon():
    b.json_target_dir = b.main_dir + b.THERE + b.JSON_SOURCE_DIR + b.THERE + b.AZON_DIR
    b.ir_target_dir = b.main_dir + b.THERE + b.RTUD_DIR + b.THERE + b.AZON_DIR

def set_bookdepository():
    b.json_target_dir = b.main_dir + b.THERE + b.JSON_SOURCE_DIR + b.THERE + b.BOOKDEPOSITORY_DIR
    b.ir_target_dir = b.main_dir + b.THERE + b.RTUD_DIR + b.THERE + b.BOOKDEPOSITORY_DIR

setter = {
    'azon' : set_azon,
    'bookdepository' : set_bookdepository,
}

class Converter(MRJob):
    def drop_json_top(self):
        f = open(b.json_target_file,"a", encoding="utf-8")
        f.write("{\n")
        f.write("\t\"Store\": " + "\"" + b.area + "\",\n")
        f.write("\t\"Books\": [\n")
        f.close()
        #self.make_top( '.json')

    def drop_json_bot(self):
        f = open(b.json_target_file,"a", encoding="utf-8")
        f.write("\t{\n\t\t\"Count\": " + "\"" + str(b.count_lines) + "\"\n\t}\n")
        f.write("]\n")
        f.write("}\n")
        f.close()
        #self.make_bot( '.json')

    def drop_json_top_ir(self, file_name):
        f = open(file_name,"a", encoding="utf-8")
        f.write("{\n")
        f.write("\t\"Store\": " + "\"" + b.area + "\",\n")
        f.write("\t\"Books\": [\n")
        f.close()
        #self.make_top( '.json')

    def drop_json_bot_ir(self, file_name):
        f = open(file_name,"a", encoding="utf-8")
        f.write("\t{\n\t\t\"Count\": " + "\"" + str(b.count_lines) + "\"\n\t}\n")
        f.write("]\n")
        f.write("}\n")
        f.close()
        #self.make_bot( '.json')

    def drop_data_as_json(self, file_name, title_name, pieces):
        f = open(file_name, 'a', encoding='utf-8')
        f.write("\t\t{\n")
        f.write("\t\t\"Name\": \"" + title_name[0].split(':')[0] + "\",\n")
        if title_name[1]:
            f.write("\t\t\"Part\": \"" + str(title_name[1]) + "\",\n")
        else:
            f.write("\t\t\"Part\": \"" + "" + "\",\n")
        f.write("\t\t\"properties\": {\n")
        for i in range(0, len(pieces) - 1):
            l = pieces[i]  
            lfirst = l[0].replace('\n', '')
            lfirst = re.sub(r'\s+', ' ', lfirst)
            lsecond = l[1].replace('\n', '')
            lsecond = re.sub(r'\s+', ' ', lsecond)
            sep = ','
            if i == len(pieces) - 2:
                sep = ''
            f.write("\t\t\t\"" + lfirst + "\": \"" + lsecond +"\"" + sep + "\n")
        f.write("\t\t}\n\t},\n")
        f.close()
        
    def convert_proops(self, pieces):
        tr = t.Translator(b.tr_mapfile)
        simul = []
        for proop in pieces:
            if proop == None:
                continue
            lfirst = proop[0]
            lsecond = proop[1]
            lfirst = lfirst.replace('\n', '')
            lfirst = re.sub(r'\s+', ' ', lfirst)
            lsecond = lsecond.replace('\n', '')
            lsecond = re.sub(r'\s+', ' ', lsecond)
            simul.append((tr.translate(lfirst), lsecond))
        return simul

    def mapper_html2json(self, _, line_path):
        downloader = d.Downloader(line_path)
        downloader.run()
        if downloader.pieces:
            for piece in downloader.pieces:
                yield downloader.title_name, piece
        else:
            yield downloader.title_name, None

    def reducer_html2json(self, title_name, pieces):
        pieces = list(pieces)
        self.drop_data_as_json(b.json_target_file, title_name, pieces)
        yield title_name, pieces

    def reducer_json2jsonir(self, title_name, pieces):
        pieces = list(pieces)
        pieces = self.convert_proops(pieces[0])
        self.drop_data_as_json(b.ir_target_file, title_name, pieces)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_html2json, reducer=self.reducer_html2json),
            MRStep(reducer=self.reducer_json2jsonir)
        ]


b.main_dir = os.getcwd()
b.tr_mapfile = b.main_dir + '/' + 'word_map.txt'

f = sys.argv[1]
f = open(f, 'r')
content = f.readlines()
b.count_lines = len(content)
b.area = content[0].split('/')[2]
f.close()

setter[b.area]() 

b.json_target_file = b.json_target_dir + b.THERE + b.area + '.json'
b.ir_target_file = b.ir_target_dir + b.THERE + b.area + '.json'

f = open(b.json_target_file,'w+')
f.close()

f = open(b.ir_target_file,'w+')
f.close()

converter = Converter()
converter.drop_json_top()
converter.drop_json_top_ir(b.ir_target_file)
converter.run()
converter.drop_json_bot()
converter.drop_json_bot_ir(b.ir_target_file)
