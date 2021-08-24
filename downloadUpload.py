import sharepy
import pandas as pd
import os
from datetime import date


username = "jlondonob@eafit.edu.co"
password = "lqpNMA20"
site_name = "WebScrapingMercadoInmobiliario"
doc_library = "Test_databases"
base_path = "https://eafit.sharepoint.com"

doc_library_backup = "Backup"



s = sharepy.connect("eafit.sharepoint.com",username=username, password=password)

#--------------------------------------------------------#
#---------| Dowloading cummulative data file | ----------#
#--------------------------------------------------------#

# Downloading files

file_to_download = "database.csv"
download_url =  base_path + "/sites/" + site_name + "/" + doc_library+ "/" + file_to_download

s.getfile(download_url)

print("Downloading database")

#--------------------------------------------------------#
#----------| Aggregation for time on market | -----------#
#--------------------------------------------------------#
# Later on can make a file for duplicate removal for or -#
# -der + more thorough duplicate removal                 #

#Read data from FincaRaiz

print("Loading database")
data_cummulative = pd.read_csv("database.csv")
print("Loading scraped data")
data_new = pd.read_csv("scrapySpider/new_data.csv")

data_cummulative = data_cummulative.append(data_new, ignore_index=False)

#Change NA for -1. Aggregation deletes rows with NA
data_cummulative = data_cummulative.fillna(-1)

#Remove variables for aggregation purposes. They are added back later
vars = data_cummulative.columns.tolist()
vars_to_remove = ['firstCapture','lastCapture']
vars = [var_kept for var_kept in vars if var_kept not in vars_to_remove] 

#Aggregate data
data_cummulative = data_cummulative.groupby(vars, as_index=False).agg({'firstCapture':'first', 'lastCapture': 'last' })

#Calculate time in the market as difference (days) between first and last capture
data_cummulative['firstCapture'] = pd.to_datetime(data_cummulative['firstCapture'])
data_cummulative['lastCapture'] = pd.to_datetime(data_cummulative['lastCapture'])
data_cummulative['timeMarket'] = (data_cummulative['lastCapture'] - data_cummulative['firstCapture']).dt.days + 1

#Write file
data_cummulative.to_csv("database_done.csv", index=False)

print("Aggregation complete, data written")
#--------------------------------------------------------#
#--------| Uploading new cummulative data file | --------#
#--------------------------------------------------------#

# Writing new database to cloud

print("Uploading new database")
file_name = "database.csv"
destination_path =  base_path + "/sites/" + site_name + "/_api/web/GetFolderByServerRelativeUrl('" + doc_library + "')/Files/add(url='"+file_name+"',overwrite=true)"

file_to_upload = "database_done.csv"
with open(file_to_upload, 'rb') as read_file:
    content = read_file.read()
s.post(destination_path, data=content)

# Uploading backup to cloud
print("Uploading backup")

file_name = "backup" + date.today().strftime("%Y-%m-%d") + ".csv"
destination_path =  base_path + "/sites/" + site_name + "/_api/web/GetFolderByServerRelativeUrl('" + doc_library_backup + "')/Files/add(url='"+file_name+"',overwrite=true)"

file_to_upload = "scrapySpider/new_data.csv"
with open(file_to_upload, 'rb') as read_file:
    content = read_file.read()
s.post(destination_path, data=content)

print("Process complete")

# Deleting extra file to not overwrite next time
os.remove("scrapySpider/new_data.csv")  
os.remove("database_done.csv")
os.remove("database.csv")