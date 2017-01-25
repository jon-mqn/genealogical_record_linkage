from __future__ import division
import re
import os
import time
import fnmatch
import gzip
from random import shuffle
import smtplib
import sys



# In[15]:

def send_mail(recip, body):
    smtpObj = smtplib.SMTP('smtp.gmail.com', 587)
    smtpObj.ehlo()
    smtpObj.starttls()
    smtpObj.login(' byupython@gmail.com ', ' oEGe2n0OAKct ')
    sig = "\n-Jon's Python Bot"
    #body = body + sig
    print body
    smtpObj.sendmail(' byupython@gmail.com ', recip, body)
    smtpObj.quit()
    return


# In[3]:

def ungzip(gz):
    inF = gzip.open(gz, 'rb')
    outF = open(gz[:-3], 'wb')
    try:
        outF.write( inF.read() )
    except:
        print 'failed'
        pass
    inF.close()
    outF.close()


# In[4]:

def write_file(path, dir):
    #initialize summary variables 
    lines_written = 0
    failed = 0
    ark_found = 0
    gender_found = 0
    given_name_found = 0
    surname_found = 0
    census_place_found = 0
    census_next_line = 0
    birth_info_soon = 0
    birth_year_found = 0
    birth_place_found = 0
    fs_record_id_soon = 0
    fs_record_id_found = 0
    race_soon = 0
    race_found = 0
    relationship_soon = 0
    relationship_found = 0
    
    
    output = dir + '/done/' + path + '_processed.txt'
    loop_start = time.time()
    with open(path, 'r+') as infile, open(output, 'w') as output_file:
        #initialize the variables
        arkid=unique_identifier=fs_record_id=given_name=surname=birth_year=birth_place=gender=race=relationship=census_place=" "
        #write the first line of the output file
        first_line = 'file|arkid|fs_record_id|given_name|surname|birth_year|birth_place|gender|race|relationship|census_place\n'
        output_file.write(first_line)
        for line in infile:
            #remove leading spaced
            line = line.lstrip()
            #finds the fs id
            if line[0:4] == '<ide' and ark_found == 0:
                arkid = re.search(r"([0-9A-Z]{4}-[0-9A-Z]{3})",line)
                if arkid:
                    arkid = arkid.group(1)
                    ark_found = 1
                else:
                    continue
            #now find gender, make it binary
            if line[1:7] == 'gender' and gender_found == 0:
                gender = re.search(r"/(Female|Male)",line)
                if gender:
                    gender = 1 if gender.group(1)  == "Male" else 0
                    gender_found = 1
            #now find birth name
            if re.search(r"/(Given)\" value",line) and given_name_found == 0:
                given_name = re.search(r"value\=\"(.*)\">",line)
                given_name = given_name.group(1)
                given_name_found == 1
            if re.search(r"/(Surname)\" value",line) and surname_found == 0:
                surname = re.search(r"value\=\"(.*)\">",line)
                surname = surname.group(1)
                surname_found == 1
            #now find census location
            if line[0:7] == '<place>' and census_place_found == 0:
                census_next_line = 1
                continue
            if census_next_line == 1:
                census_next_line = 0
                census_place = re.search(r"<original>(.*)</original>",line)
                census_place = census_place.group(1)
                census_place_found = 1
            #now find bith year
            if re.search(r"/(Birth)\"",line) and birth_year_found == 0:
                birth_info_soon = 1
            if birth_info_soon == 1 and birth_year_found == 0:
                birth_year = re.search(r"<original>([A-Za-z0-9 ,]+)</original>",line)
                if birth_year:
                    birth_year = birth_year.group(1)
                    birth_year_found = 1
                    continue
                else:
                    birthyear = '0000'
            if birth_info_soon == 1 and birth_year_found == 1 and birth_place_found == 0:
                birth_place = re.search(r"<original>([A-Za-z0-9 ,]+)</original>",line)
                if birth_place:
                    birth_place = birth_place.group(1)
                    birth_place_found = 1
                    birth_info_soon == 0
            #now get record id that we could manipulate to group families
            if re.search(r"\"(FS_RECORD_ID)\"",line) and fs_record_id_found == 0:
                fs_record_id_soon = 1
                continue
            if fs_record_id_soon == 1 and fs_record_id_found == 0:
                fs_record_id = re.search(r"<text>([0-9_]+)</text>",line)
                if fs_record_id:
                    fs_record_id = fs_record_id.group(1)
                    fs_record_id_found = 1
                    fs_record_id_soon = 0
            
            #now get race
            if re.search(r"\"(PR_RACE_OR_COLOR)\"",line) and race_found == 0:
                race_soon = 1
                continue
            if race_soon == 1:
                race = re.search(r"<text>(.*)</text>",line).group(1)
                race_found = 1
                race_soon = 0
            #now get relationship to head
            if re.search(r"\"(PR_RELATIONSHIP_TO_HEAD)\"",line) and relationship_found == 0:
                relationship_soon = 1
                continue
            if relationship_soon == 1:
                relationship = re.search(r"<text>(.*)</text>",line).group(1)
                relationship_found = 1
                relationship_soon = 0
            
            #now to find the end of a persons record, write to csv, reinitialize starting vars
            if re.search(r"(</person>)",line):
                lines_written = lines_written + 1
                try: 
                    out_line = path + '|' + str(arkid) + '|' + str(fs_record_id) + '|' + str(given_name) + '|' + str(surname) + '|' + str(birth_year) + '|' + str(birth_place) + '|' + str(gender) + '|' + str(race) + '|' + str(relationship) + '|' + str(census_place) + '\n'
                    output_file.write(out_line)
                except: 
                    failed = failed + 1
                ark_found = 0
                gender_found = 0
                given_name_found = 0
                surname_found = 0
                census_place_found = 0
                census_next_line = 0
                birth_info_soon = 0
                birth_year_found = 0
                birth_place_found = 0
                fs_record_id_soon = 0
                fs_record_id_found = 0
                race_soon = 0
                race_found = 0
                relationship_soon = 0
                relationship_found = 0
                
    loop_end = time.time() 
    loop_elapsed = loop_end - loop_start

    #summary
    print '\nsummary:\n'
    print 'file: ' + path
    print 'time elapsed: %.2f'%(loop_elapsed)
    print 'lines written: %d'%(lines_written)
    print 'failed: %d\n'%(failed)
   


# In[ ]:

#find all gz files, unzip, run through each file, delete once done
dir = 'R:/JoePriceResearch/record_linking/data/census_1910/'
os.chdir(dir)
#for this, we will have the master (0) and three bots (1-3). bot 3 will handle the remainder
bot = int(sys.argv[1])
length = 621
update_length = 200
start = 75560
end = 81772
#find zip files
bot_start = bot*length + start
bot_end = bot_start + length - 1
begin = time.time() 
for i in range(bot_start,bot_end):
    gz = str(i) + '.gz'
    print 'working on ' + gz
    ungzip(gz)
    txt = gz[:-3]
    write_file(txt, dir)
    os.remove(txt)
    done = i - bot_start + 1
    remaining = bot_end - i
    average_time = (time.time() - begin)/(60*done)
    time_remaining = average_time*remaining
    print 'done: %d'%(done)
    print 'remaining: %d'%(remaining)
    print 'average time: %.2f'%(average_time)
    print 'time remaining: %.2f\n'%(time_remaining)
    #now email me an update after a specified benchmark
    if done%update_length == 0 and remaining > 0:
        subject = 'Subject: Update from bot %d (1910) \n'%(bot)
        body = subject + 'done: %d\nremaining: %d\naverage time: %.2f\ntime remaining: %.2f'%(done,remaining,average_time,time_remaining)
        send_mail('jon@mcewan.me', body)
        print 'email sent\n'

time_elapsed = (time.time() - begin)/60
print 'time elapsed: %.2f'%(time_elapsed)
#send update when done, log off
subject = 'Subject: bot %d is finished\n'%(bot)
body = subject + 'done: %d\ntime elapsed: %.2f\naverage time: %.2f'%(done,time_elapsed,average_time)
print body
send_mail('jon@mcewan.me', body)
os.system("shutdown -l")
    
      
        
    


# In[ ]:




# In[12]:




# In[16]:



