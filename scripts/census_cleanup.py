# -*- coding: utf-8 -*-
"""
Created on Thu Jan 12 12:07:04 2017

@author: tannerse
"""

import os
import gzip
import re
import subprocess
import socket

directory = 'R:\\JoePriceResearch\\record_linking\\data\\census_1910'
os.chdir(directory)


count = 1
for filename in os.listdir(directory):
    if filename.endswith('.gz'):
        number = int(filename[:-3])
        if number <= 73903:
            continue
        print(filename)
        filenew = directory + '\\data\\' + filename[:-3] + '.txt'
        print(filenew)
        with gzip.open(filename, 'rb') as f:
            file_content = f.read()
            total = []
            y = 10000000
            for x in range(0,len(file_content),10000000):
                temp = re.sub(r'\n',r'',file_content[x:y])
                temp = re.sub(r'person principal',r'\n',temp)
                temp = re.sub(r'[\$]',r'',temp)
                temp = re.sub(r'labelId',r'$',temp)
                temp = re.sub(r'[<]',r'',temp)
                temp = re.sub(r'[>]',r'',temp)
                temp = re.sub(r'[=]',r'',temp)
                temp = re.sub(r'["]',r'',temp)
                total.append(temp)
                y = y + 10000000

            text = ''
            for x in range(0,len(total)):
                text = text + total[x]

            text = re.sub(r'person description',r'\n',text)
            text = re.sub(r'labelId',r'$',text)
   
            try:
                os.remove(filenew)
            except:
                pass
            j = open(filenew,'a')
            j.write(text)
            j.close()

        
        dofile = 'R:/JoePriceResearch/record_linking/data/census_1910/cleaning_scripts/census_compile.do'
        if socket.gethostname() == 'FHSSCOMPUTE':
            cmd = ['C:\\Program Files (x86)\\Stata14\\StataMP-64.exe', 'do', dofile, filenew, str(count)]
        else:
            cmd = ['C:\\Program Files (x86)\\Stata14\\StataSE-64.exe', 'do', dofile, filenew, str(count)]
        subprocess.call(cmd)
        
        os.remove(filenew)        
        
        count = count + 1
