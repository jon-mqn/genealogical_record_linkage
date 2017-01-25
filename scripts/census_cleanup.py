# -*- coding: utf-8 -*-
"""
Created on Thu Jan 12 12:07:04 2017

@author: tannerse
"""

from __future__ import division
import os
import fnmatch
import gzip
import re
import subprocess
import socket

directory = '/path/to/raw/data'
os.chdir(directory)
files = fnmatch.filter(directory,'*.gz')

count = 1
for filename in files:
    filenew = directory + '\\data\\' + filename[:-3] + '.txt'
    with gzip.open(filename, 'rb') as f, open(filenew,'a+') as j:
        file_content = f.read()
        total = []
        chunk_size = 10000000
        for x in range(0,len(file_content),chunk_size):
            temp = re.sub(r'\n',r'',file_content[x:chunk_size])
            temp = re.sub(r'person principal',r'\n',temp)
            temp = re.sub(r'[\$]',r'',temp)
            temp = re.sub(r'labelId',r'$',temp)
            temp = re.sub(r'[<]',r'',temp)
            temp = re.sub(r'[>]',r'',temp)
            temp = re.sub(r'[=]',r'',temp)
            temp = re.sub(r'["]',r'',temp)
            total.append(temp)
            y = y + 10000000
            
        text = ''.join(total)
        text = re.sub(r'person description',r'\n',text)
        text = re.sub(r'labelId',r'$',text)
   
        try:
            os.remove(filenew)
        except:
            pass
        j.write(text)
    
    #run do file Stata    
    dofile = 'C:/path/to/census_compile.do'
    #assuming Stata path is the following
    cmd = ['C:\\Program Files (x86)\\Stata14\\StataSE-64.exe', 'do', dofile, filenew, str(count)]
    subprocess.call(cmd)
    os.remove(filenew)        
    count += 1
    complete = count / len(files)
    print 'Files processed: %d'%(count/)"
    print 'Percent complete: %.2f'%(complete)\n"
