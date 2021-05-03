from mrjob.job import MRJob
from mrjob.job import MRStep
import bridge as b
import Downloader as d
import Translator as t
import sys
import os
import re
import json

class Merger(MRJob):
    def push_head(self):
        f = open(b.main_dir + b.THERE + b.very_target_file, 'w', encoding='utf-8')
        f.write("{\n")
        f.write("\t\"Movies\": [\n")
        f.close()

    def push_tail(self):
        f = open(b.main_dir + b.THERE + b.very_target_file, 'a', encoding='utf-8')
        f.write("\t{\n\t\t\"Count\": " + "\"" + str(b.count_lines) + "\"\n\t}\n")
        f.write("]\n")
        f.write("}\n")
        f.close()

    def push_into_file(self, title_name, pieces):
        f = open(b.main_dir + b.THERE + b.very_target_file, 'a', encoding='utf-8')
        f.write("\t\t{\n")
        f.write("\t\t\"Name\": \"" + title_name.split(':')[0] + "\",\n")
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

    def pull_props(self, data):
        props = []
        pieces = []
        name = None

        f1 = open(b.main_dir + '/' + b.RTUD_DIR + b.THERE + b.AZON_DIR + b.THERE + b.AZON_DIR + ".json", "r",  encoding='utf-8')
        data1 = json.load(f1)
        f1.close()
        for book in range(0, len(data1['Books']) - 1):
            name = data1['Books'][book]["Name"].split('–')[0].split(':')[0]
            if name.replace(' ', '') == data[0].replace(' ', ''):
                props.append((book, data1))
                break
        if len(props) == 0:
            props.append(None)

        pieces = []
        f2 = open(b.main_dir + '/' + b.RTUD_DIR + b.THERE + b.BOOKDEPOSITORY_DIR + b.THERE + b.BOOKDEPOSITORY_DIR + ".json", "r")
        data2 = json.load(f2)
        f2.close()
        for book in range(0, len(data2['Books']) - 1):
            name = data2['Books'][book]["Name"].split('–')[0].split(':')[0]
            if name.replace(' ', '') == data[1].replace(' ', ''):
                props.append((book, data2))
                break
        if len(props) == 1:
            props.append(None)
        return props[0], props[1]

    def merge(self, props1, props2):
        props = []
        if props1 != None:
            for k in props1[1]['Books'][props1[0]]['properties'].keys():
                prop, value = k, props1[1]['Books'][props1[0]]['properties'][k]
                props.append((prop, value))
        if props2 != None:
            for k in props2[1]['Books'][props2[0]]['properties'].keys():
                prop, value = k, props2[1]['Books'][props2[0]]['properties'][k]
                props.append((prop, value))

        return props

    def mapper(self, _, line):
        data = line.split('|')
        props1, props2 = self.pull_props(data)
        props = self.merge(props1, props2)
        yield data[0], props
            
    def reducer(self, name, props):
        props = list(props)
        fhp = open(b.main_dir + '/' + "s.txt", "a",  encoding='utf-8')
        fhp.write(str(props))
        fhp.close()
        self.push_into_file(name, props[0])

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
        ]

b.main_dir = os.getcwd()
b.tr_mapfile = b.main_dir + '/' + 'word_map.txt'

merger = Merger()
merger.push_head()
merger.run()
merger.push_tail()
