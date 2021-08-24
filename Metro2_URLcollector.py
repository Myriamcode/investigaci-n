#--------------------------------------------------------------------------------------------------#
#-----------------------| METRO 2 URL SCRAPER SETUP |----------------------------------------------#
#--------------------------------------------------------------------------------------------------#
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait             #wait until javascript data loads
from selenium.webdriver.common.keys import Keys                     #simulate pressing keys or buttons in webpage
from selenium.webdriver.chrome.options import Options               #add headless option to driver
from selenium.webdriver.support import expected_conditions as EC    #used with webdriverwait to wait until a certain condition is met
from selenium.webdriver.common.by import By    
from selenium.webdriver.support.ui import Select

import json
import platform                                                     #to check OS (important for pressing command or control keys)




driver_path = '/Users/puchu/Desktop/WebScraper_Metro2/chromedriver'
chrome_options = Options()
chrome_options.add_argument('--headless')
webdriver = webdriver.Chrome(
    executable_path=driver_path,
    #options=chrome_options
)
all_urls = []

base_url = "https://www.metrocuadrado.com"
#possibilities: "apartamento" and "casa"
housing_type = ["apartamento", "casa"] 
#possibilities: "medellin","bogota", "barranquilla", "cali"
#cities = ["cali", "medellin","bogota", "barranquilla"]
cities = ["medellin"]


home_page = base_url + '/' + "-".join(housing_type) + '/venta/'
#casa primero luego apartamento

price_dividers = {
            'bogota': [f"{price}000000" for price in ["0", "200", "300", "400", "500", "600", "700","800","900","1000","1400","1700","2500","10000"]],
            'medellin': [f"{price}000000" for price in ["0", "500", "10000"]],
        }
#--------------------------------------------------------------------------------------#
#-----------------------| SCRAPER BODY |-----------------------------------------------#
#--------------------------------------------------------------------------------------#
with webdriver as driver:

    wait = WebDriverWait(driver, 100)

    index = 0
    
    for city in cities:

        driver.get(home_page + city)  

        #accept cookies to remove banner (only in first run)
        if index==0:
            accept_cookies_button = driver.find_element_by_css_selector('.disclamer-action a')
            accept_cookies_button.click() 

        #buttons
        min_price = driver.find_element_by_xpath("//input[@name='startPrice']")
        max_price = driver.find_element_by_xpath("//input[@name='endPrice']")
        filter_price_button = driver.find_element_by_id("filter-price")
        
       

        if city not in price_dividers:
            price_dividers[f"{city}"] = [f"{price}000000" for price in ["0", "10000"]]
        
        #loop through possible prices. Minus 1 because we don't want last divider as minimum price.
        for i in range(len(price_dividers[f"{city}"])-1):
            
            #Deletes prior input
            if platform.system()=="Darwin":         #Mac OS
                select_all = Keys.COMMAND + "a"
            elif platform.system()=="Windows":      #Windows OS
                select_all = Keys.CONTROL + "a"
            min_price.send_keys(select_all)
            min_price.send_keys(Keys.DELETE)
            max_price.send_keys(select_all)
            max_price.send_keys(Keys.DELETE)
            ###   
            min_price.send_keys(price_dividers[f"{city}"][i])
            max_price.send_keys(price_dividers[f"{city}"][i+1])
            filter_price_button.click()

            #---
            #CODE TO GO TRHOUGH PAGES AND SCRAPE
            #---

            next_page_button = driver.find_element_by_css_selector('.item-icon-next a')
            arrow_disabled = driver.find_elements_by_css_selector('.item-icon-next.page-item.disabled')

            links_main = driver.find_elements_by_css_selector('.card-result-img .sc-bdVaJa')
            links_links_mainmain_divided = [link.get_attribute('href') for link in links_main]

            while True:
                if len(arrow_disabled) > 0:
                    print('No more pages left')
                    break
                else:
                    # Clicks next page and checks if button is disabled
                    next_page_button.click()
                    arrow_disabled = driver.find_elements_by_css_selector('.item-icon-next.page-item.disabled')

                    # Only purpose of this chunk is testing. It may be deleted
                    current_page=driver.find_element_by_css_selector('.page-item.active')

                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.card-result-img .sc-bdVaJa')))

                    # Gets links from specific page
                    links_next_page = driver.find_elements_by_css_selector('.card-result-img .sc-bdVaJa')
                    links_next_page = [link.get_attribute('href') for link in links_next_page]

                    print('Number of urls scraped at page' + current_page.text + ':' + str(len(links_next_page)))

                    # Appends new links to links object
                    links_main.extend(links_next_page)
            all_urls.extend(links_main)
        index +=1
        
    driver.close()

with open('collectedURLSbog.txt','w') as fp:
    json.dump(all_urls,fp)