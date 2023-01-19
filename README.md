# Flickr
Project For Data Loops 
##this is the first version 

- main.py 
   -   class DBconnector ( creates a mysql connection ) 
   -   FlickrImageDownload ( scrape and downloads from Flicker )
   -   Main ( Run Scrape Sample , and Quaery sample )       
   
-Saved parameters (ini file )



# pre-required :

# mysql Image - with Db called Flickr , Table- 
CREATE TABLE `images` (
  `scrape_datetime` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `url` varchar(400) DEFAULT NULL,
  `file_loc` varchar(400) DEFAULT NULL,
  `hashed_sha256` char(64) DEFAULT NULL,
  `keyword` varchar(100) DEFAULT NULL
)

# Flickr Api key updated in ini file 
# paths to update in ini file 

