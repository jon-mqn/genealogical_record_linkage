*code to bring in the census data
*written by Jon McEwan

clear
set more off, perm
cd /path/to/processed/files

local i = 1
local textfiles: dir . files "*_processed.txt"
foreach txt of local textfiles{
	insheet using `txt', clear delim("|") names
	qui tostring _all, replace 
	qui for X in varlist _all: qui cap replace X = lower(X)
	rename arkid ark1910
	replace ark1910 = upper(ark1910)
	save R:\JoePriceResearch\record_linking\data\census_1910\done\statafiles/`i'.dta, replace
	local i = `i' + 1
}
