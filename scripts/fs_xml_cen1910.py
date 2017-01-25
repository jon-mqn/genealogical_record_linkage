from __future__ import division
import re
import os
import time
import fnmatch
import gzip

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

def write_file(path, dir):
    #initialize summary variables 
    lines_written=failed=ark_found=gender_found=given_name_found=surname_found=census_place_found=census_next_line=birth_info_soon=0
    birth_year_found=birth_place_found=fs_record_id_soon=fs_record_id_found=race_soon=race_found=relationship_soon=relationship_found=0
    output = dir + '/done/' + path + '_processed.txt'
    loop_start = time.time()
    with open(path, 'r+') as infile, open(output, 'w') as output_file:
        #initialize the variables
        arkid=unique_identifier=fs_record_id=given_name=surname=birth_year=birth_place=gender=race=relationship=census_place=" "
        #write the first line of the output file
        first_line = 'file|arkid|fs_record_id|given_name|surname|birth_year|birth_place|gender|race|relationship|census_place\n'
        output_file.write(first_line)
        for line in infile:
            #remove leading spaces
            line = line.lstrip()
            #finds the arkid
            if line[0:4] == '<ide' and ark_found == 0:
                arkid = re.search(r"([2-9A-Z]{4}-[0-9A-Z]{3})",line)
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
                    failed += 1
                lines_written=failed=ark_found=gender_found=given_name_found=surname_found=census_place_found=census_next_line=birth_info_soon=0
                birth_year_found=birth_place_found=fs_record_id_soon=fs_record_id_found=race_soon=race_found=relationship_soon=relationship_found=0
                
    loop_end = time.time() 
    loop_elapsed = loop_end - loop_start

    #summary
    print '\nsummary:\n'
    print 'file: ' + path
    print 'time elapsed: %.2f'%(loop_elapsed)
    print 'lines written: %d'%(lines_written)
    print 'failed: %d\n'%(failed)
   
def main():
	dir = 'R:/JoePriceResearch/record_linking/data/census_1910/'
	os.chdir(dir)
	#find zip files
    files = fnmatch.filter(dir,'.gz')
	begin = time.time() 
    i = 0
	for file in files:
		print 'working on ' + file
		ungzip(file)
		txt = file[:-3]
		write_file(txt, dir)
        i += 1
		os.remove(txt)
		remaining = len(files) - i
		average_time = (time.time() - begin)/(60*i)
		time_remaining = average_time*remaining
		print 'done: %d'%(done)
		print 'remaining: %d'%(remaining)
		print 'average time: %.2f'%(average_time)
		print 'time remaining: %.2f\n'%(time_remaining)

	time_elapsed = (time.time() - begin)/60
	print 'time elapsed: %.2f'%(time_elapsed)

if __name__ == "__main__":
    main()
