from threading import Thread
import requests
import json
import resource
from BeautifulSoup import BeautifulSoup
from pprint import pprint

class WikiException(Exception):
    pass

class WikiScraper(Thread):
    data = {}
    parser = None
    response = None


    def __init__(self, subject):
        self.subject = subject
        try:
            self.connect("http://en.wikipedia.org/wiki/Category:"+subject)
        except WikiException:
            raise WikiException("Subject does not exist")
        super(WikiScraper, self).__init__()

    def run(self):
        self.scrape_recursive()

    def connect(self, url):
         self.response = requests.get(url, timeout=1)
         if self.response.status_code != 200:
             raise WikiException("Invalid response code recieved")
         self.parser = BeautifulSoup(self.response.text)
   
    def get_category_links(self):
        links = []
        categories = self.parser.findAll("div", attrs={"id":"mw-subcategories"})
        if categories:
            categories = categories[0]
        else:
            return links

        for category_link in categories.findAll("a", href=True):
            links.append("http://en.wikipedia.org"+category_link['href'])
        return links
   
    def print_status(self):
        print "Scraping wiki subject: %s" % self.subject
        print 'Memory usage: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
         
    def scrape_recursive(self):
        """Obtains all of the wiki links for a given subject and all of the links 
        for each sub-category of a given subject"""
        #Get pages for current subject
        self.scrape()
        self.print_status()
        #Get pages for each sub-category
        unscraped_links = self.get_category_links()
        scraped_links = set([])

        while(unscraped_links):
            for link in unscraped_links:
                self.subject = link.split(':')[2]
                self.connect(link)
                self.scrape()
                self.print_status()
                unscraped_links.remove(link)
                scraped_links.add(link)
                for link in self.get_category_links():
                    if link not in scraped_links:
                        unscraped_links.append(link)
                

    def scrape(self):
        """Obtains all of the wiki links for a given subject"""
        pages = self.parser.findAll("div", attrs={"id":"mw-pages"})

        if pages:
            links = pages[0].findAll("div", attrs={"class":"mw-content-ltr"})[0]

            self.data[self.subject] = []
            for link in links.findAll("a", href=True):
                self.data[self.subject].append(link['href'])

    def dump_data_to_file(self, filename):
        with open(filename, "w") as f:
            json.dump(self.data, f)


#Example usages
def threaded_scrape():
    scraper = WikiScraper("Internet_search_engines")
    init_links = scraper.get_category_links()

    threads = [] 
    for link in init_links:
        worker_subject = link.split(':')[2]
        t = WikiScraper(worker_subject)
        t.start()
        threads.append(t)

    #wait for all threads to finish
    for thread in threads:
        thread.join()
    print "Exiting main thread.."
   
def non_threaded_scrape():
    scraper = WikiScraper("Internet_search_engines")
    scraper.scrape_recursive()

if __name__ == "__main__":
    threaded_scrape()
