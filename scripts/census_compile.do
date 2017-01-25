/*
Author: Tanner Eastmond
Date: 1/12/17
Data: *.txt
*/

local filename = "`1'"

cd "R:\JoePriceResearch\record_linking\data\census_1910\data"
set more off, perm
insheet using `filename', clear delim($)

gen filename = "`filename'"
drop if _n == 1
gen arkid = regexs(0) if regexm(v1,"tenthttps.*ark.*1:1:[A-Z0-9]+-[A-Z0-9]+")
replace arkid = substr(arkid,5,.)
drop v1
foreach x of varlist _all{
	gen temp = substr(`x',1,strpos(`x'," "))
	levelsof temp, local(names)
	foreach y of local names{
		cap gen `y' = ""
	}
	drop temp
}

ds v*, not
foreach x of varlist `r(varlist)'{
	ds v*
	foreach y of varlist `r(varlist)'{
		replace `x' = `y' if regexm(`y',"`x'")
		replace `x' = regexs(0) if regexm(`x',"text.*/text")
	}
	replace `x' = subinstr(`x',"text","",.)
	replace `x' = subinstr(`x',"/","",.)
}
drop v*

compress
cap append using temp
save temp, replace

exit, STATA clear
