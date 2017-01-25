*merge together some true pairs

*prep  make two pairs datasets
cd R:\JoePriceResearch\record_linking\projects\deep_learning
use ark_pairs, clear
rename ark1 ark1900
rename ark2 ark1910
save ark_pairs1, replace
rename ark1900 ark
rename ark1910 ark1900
rename ark ark1910
save ark_pairs2, replace

global deeplearn "R:\JoePriceResearch\record_linking\projects\deep_learning"
global ark1 "R:\JoePriceResearch\record_linking\projects\deep_learning\ark_pairs1"
global ark2 "R:\JoePriceResearch\record_linking\projects\deep_learning\ark_pairs2"
global cen1900 "R:\JoePriceResearch\record_linking\data\census_1900\census1900_raw\done\statafiles"
global cen1910 "R:\JoePriceResearch\record_linking\data\census_1910\done\statafiles"

*1900
*grab some ohio
cd $cen1900
local dtas: dir . files "*.dta"
local i = 1
foreach dta of local dtas {
use `dta', clear
save sample/all`i'.dta, replace
local i = `i' + 1
}

cd sample
clear
local twostate: dir . files "all*"
foreach file of local twostate {
append using `file'
}
compress
*check duplicates
duplicates tag ark1900, gen(dup)
drop if dup > 0  
drop dup
*add a 1900 subscript to each var
foreach var of varlist _all {
	if "`var'" == "ark1900" {
		continue
	}
	rename `var' `var'1900
}
*year has month. split
gen byear1900 = regexs(1) if regexm(birth_year1900,"([0-9][0-9][0-9][0-9])")
save all1900, replace

*1910
*grab some ohio
cd $cen1910
local dtas: dir . files "*.dta"
local i = 1
foreach dta of local dtas {
use `dta', clear
save sample/all`i'.dta, replace
local i = `i' + 1
}
cd sample
clear
local all: dir . files "all*"
foreach file of local all {
append using `file'
}
compress
*check for duplicates
duplicates tag ark1910, gen(dup)
*there are a lot more duplicates and a lot fewer observations.
drop if dup > 0
drop dup
*add a 1910 subscript to each var
foreach var of varlist _all {
	if "`var'" == "ark1910" {
		continue
	}
	rename `var' `var'1910
}
save all1910, replace

*start merging
cd $deeplearn
*1900
use ${cen1900}\sample\all1900, clear
merge 1:m ark1900 using ark_pairs1
keep if _merge == 3
compress
save cenpairs1900_1, replace
use ${cen1900}\sample\all1900, clear
merge 1:m ark1900 using ark_pairs2
keep if _merge == 3
compress
save cenpairs1900_2, replace
append using cenpairs1900_1
drop _merge
save cenpairs1900, replace
*1910
use ${cen1910}\sample\all1910, clear
merge 1:m ark1910 using ark_pairs1
keep if _merge == 3
compress
save cenpairs1910_1, replace
use ${cen1910}\sample\all1910, clear
merge 1:m ark1910 using ark_pairs2
keep if _merge == 3
compress
save cenpairs1910_2, replace
append using cenpairs1910_1
drop _merge
save cenpairs1910, replace

*now merge them together 
merge m:1 ark1900 using cenpairs1900
keep if _merge == 3
drop _merge
gen similscore = 1
save all_sample_matches, replace

*so let's keep all of the 1910s and find potential matches
foreach var of varlist _all {
if strpos("`var'", "1900") {
	drop `var' 
}
}
cap drop similscore
*start blocking
gen white = race == "white"
tostring white, gen(key)
drop white
replace key = key + birth_year1910 + gender1910 + substr(given_name1910, 1, 3) + substr(surname1910, 1, 2)
save tomatch1910, replace
*1900
use ${cen1900}\sample\all1900, clear
gen white = race == "white"
tostring white, gen(key)
drop white
replace key = key + byear1900 + gender1900 + substr(given_name1900, 1, 3) + substr(surname1900, 1, 2)
joinby key using tomatch1910

*set clusters
parallel setclusters 4
gen name_1900 = given_name1900 + " " + surname1900
gen name_1910 = given_name1910 + " " + surname1910
gen sort = _n
sort sort
parallel, by(sort) : matchit name_1900 name_1910
*drop perfect matches
drop if similscore == 1
append using all_sample_matches
*now drop out the true matches that didn't get a perfect score
gsort ark1910 ark1900 -similscore
drop if ark1900 == ark1900[_n-1] & ark1910 == ark1910[_n-1] & similscore != 1
gsort ark1910 -similscore
by ark1910: gen n = _N
gen match = similscore == 1
save all_sample_match_and_potential,replace

*now get some numeric features and export csv for some ML
gen white1900 = race1900 == "white"
gen white1910 = race1910 == "white"
gen racematch = white1900 == white1910
drop byear1900
gen byear1900 = regexs(1) if regexm(birth_year1900, "([0-9][0-9][0-9][0-9])")
gen yeardif = byear1900 - birth_year1910
gen year = byear1900
*month dummys
gen month = regexs(1) if regexm(birth_year1900, "([a-z][a-z][a-z])")
tabulate month, generate(mon)
local i = 1
foreach month in ///
"apr" ///
"aug" ///
"dev" ///
"feb" ///
"jan" ///
"jul" ///
"jun" ///
"mar" ///
"may" ///
"nov" ///
"oct" ///
"sep" /// 
{
rename mon`i' `month'
local i = `i' + 1
}
*name scores
matchit given_name1900 given_name1910, gen(given_match)
matchit surname1900 surname1910, gen(surname_match)
*see if census state and county match
gen state1900 = reverse(regexs(1)) if regexm(reverse(census_place1900), ",([a-z .]+) ,")
cap gen state1910 = reverse(regexs(1)) if regexm(reverse(census_place1910), ",([a-z .]+) ,")
gen statematch = state1900 == state1910
gen county1900 = reverse(regexs(1)) if regexm(reverse(census_place1900), ",[a-z .]+ ,([a-z .]+) ,")
gen county1910 = reverse(regexs(1)) if regexm(reverse(census_place1910), ",[a-z .]+ ,([a-z .]+) ,")
gen countymatch = county1900 == county1910
gen relationshipmatch = relationship1900 == relationship1910
gen gender = gender1900

*outsheet the whole dataset
gsort -match -similscore
cd training_sets
local chunk_size = 250000
local total_files = ceil(_N / `chunk_size')
forval i = 1/`total_files' {
local j = `i' - 1
local upper = `i' * `chunk_size'
if `upper' > _N {
	local `upper' = _N
}
local lower = `j' * `chunk_size' + 1
outsheet _all using all_features_training`i'.csv in `lower'/`upper', replace delim("|")
}

keep match similscore *match gender apr aug dev feb jan jul jun mar may nov oct sep
outsheet _all using all_all_training.csv, comma
