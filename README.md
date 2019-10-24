# Block_Codes
# Step 1. Download Files.
- download_forms.R file downloads sc13d/13g files and their amendments and puts them into SQL database.
- this file downloads the list of all forms for each year from SEC website, 
  the only thing you need to specify is a range of years in loop and working directory
- code is slow and takes up to several hours to complete. To make sure, that I get all posible files, 
  I download each file twice from master file for filer and for subject.
  
# Step 2. Extract and Convert Main Filings.
- extract_body_form.R extracts main filing from complete submission files and convert .htm to plain text format if needed.
- I put output into another SQL database. 

# Step 3. Parse SEC Header.
- pasing_SEC_header.R extracts filer and subject information from the form
- This script could be used for data extraction from other forms
- I have a blog entry about this function (https://orhahog.wordpress.com/2016/11/26/parsing-sec-header/)

# Step 4. Extract CUSIP from the filings.
- extract_CUSIP.R script returns six and eight digit CUSIP from SEC filings.
- Output of this part is a CIK-CUSIP map, which could be downloaded in .csv format from my website (www.evolkova.info)

# Step 5. Extract size of the block positon.
- parsing_prc_position.R extracts the aggregate block size from the filing.

# Step 6. Extract identity of blockholders.
- parsing_block_identity.R extracts identity of blockholders from the information in the question 12.
- here is the list of all identities (https://www.law.cornell.edu/cfr/text/17/240.13d-102)
