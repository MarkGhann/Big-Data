import bridge as b
import sys
import os

from_file = b.HERE + b.JSON_SOURCE_DIR + b.THERE + b.AZON_SOURCE_FILE
os.system("python3 html2jsonir.py " + from_file)

from_file = b.HERE + b.JSON_SOURCE_DIR + b.THERE + b.BOOKDEPOSITORY_SOURCE_FILE
os.system("python3 html2jsonir.py " + from_file)

from_file = b.HERE + b.RTUD_DIR + b.THERE + b.BOOKDEPOSITORY_AIM_FILE
os.system("python3 resolve.py " + from_file)
