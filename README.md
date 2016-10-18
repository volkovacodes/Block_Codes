# Block_Codes
# Step 1. Download Files.
- download_forms.R file downloads sc13d/13g files and their amendments and puts them into SQL database.
- this file downloads the list of all forms for each year from SEC website, 
  the only thing you need to specify is a range of years in loop and working directore
- code is slow and takes up to several hours to complite. To make sure, that I get all posible files, 
  I download each file twice from master file for filer and for subject.
  
# Step 2. Extract and Convert Main Filings.
- extract_body_form.R extracts main filing from complete submission files and convert .htm to plain text format if need.
- I put output into another SQL database. 
