# https://stackabuse.com/scheduling-jobs-with-python-crontab/

# Run consecutive jobs
# https://askubuntu.com/questions/923260/cron-run-a-script-after-the-other

# pip3 install python-crontab
# job.dow.on('SUN')
# job.hour.also.on(3)

from crontab import CronTab

cron = CronTab(user='puchu')
job = cron.new(command='cd /Users/puchu/Desktop/WebScraper_Metro2/scrapySpider && /Library/Frameworks/Python.framework/Versions/3.8/bin/scrapy crawl FincaRaiz -o csv.csv >> /Users/puchu/Desktop/WebScraper_Metro2/output.log 2>&1',
               comment="FincaRaiz")

#run code on saturdays
job.dow.on('SAT')
job.hour.also.on(0)
job.minute.also.on(0)

cron.write()


#USEFUL COMMANDS
#----------------
#cron.remove_all()
#job.clear()
#job.enable()
#job.enable(False)
#job.is_enabled()
#cron.remove(job)
