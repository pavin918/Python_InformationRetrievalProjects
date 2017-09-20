import logging
from datamodel.search.datamodel import ProducedLink, OneUnProcessedGroup, robot_manager
from spacetime_local.IApplication import IApplication
from spacetime_local.declarations import Producer, GetterSetter, Getter
# from lxml import html,etree
import re, os
from time import time
from lxml import etree, html
from bs4 import BeautifulSoup
from collections import Counter
try:
    # For python 2
    from urlparse import urlparse, parse_qs, urljoin
except ImportError:
    # For python 3
    from urllib.parse import urlparse, parse_qs

logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"
url_count = (set()
             if not os.path.exists("successful_urls.txt") else
             set([line.strip() for line in open("successful_urls.txt").readlines() if line.strip() != ""]))
MAX_LINKS_TO_DOWNLOAD = 3000
#globals
num_of_invalids = 0
most_outlinks_page = {0:set()}
subdomain_map = Counter()


@Producer(ProducedLink)
@GetterSetter(OneUnProcessedGroup)
class CrawlerFrame(IApplication):
    def __init__(self, frame):
        self.starttime = time()
        # Set app_id <student_id1>_<student_id2>...
        self.app_id = "68414541_30658761_50051833"

        # Set user agent string to IR W17 UnderGrad <student_id1>, <student_id2> ...
        # If Graduate studetn, change the UnderGrad part to Grad.
        self.UserAgentString = "IR W17 UnderGrad 68414541, 30658761, 50051833"

        self.frame = frame
        assert (self.UserAgentString != None)
        assert (self.app_id != "")
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def initialize(self):
        self.count = 0
        l = ProducedLink("http://www.ics.uci.edu", self.UserAgentString)
        print l.full_url
        self.frame.add(l)

    def update(self):
        for g in self.frame.get(OneUnProcessedGroup):
            print "Got a Group"
            outputLinks, urlResps = process_url_group(g, self.UserAgentString)
            for urlResp in urlResps:
                if urlResp.bad_url and self.UserAgentString not in set(urlResp.dataframe_obj.bad_url):
                    urlResp.dataframe_obj.bad_url += [self.UserAgentString]
            for l in outputLinks:
                if is_valid(l) and robot_manager.Allowed(l, self.UserAgentString):
                    lObj = ProducedLink(l, self.UserAgentString)
                    self.frame.add(lObj)
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def shutdown(self):
        elapsed_time = time() - self.starttime
        print "downloaded ", len(url_count), " in ", elapsed_time, " seconds."
        avg_time = elapsed_time / len(url_count)
        with open("Analytics.txt", "a") as analytics:
            analytics.write("List of subdomains and how many times they were visited:\n")
            for subdomain in subdomain_map.most_common():
                analytics.write(str(subdomain[0]) + ": "+ str(subdomain[1]) + "\n")
            analytics.write("\n")
            analytics.write("Number of invalids links: " + str(num_of_invalids) + "\n\n")
            analytics.write("Set of pages with the most outgoing links:\n")
            for page_count in most_outlinks_page:
                analytics.write("\n".join(most_outlinks_page[page_count]) + \
                " has " + str(page_count) + " links" + "\n")
            analytics.write("\n")
            analytics.write("Average download time per URL: " + str(avg_time) + " seconds.\n\n")
            analytics.write("Total download time: " + str(elapsed_time))
        pass


def save_count(urls):
    global url_count
    url_count.update(set(urls))
    with open("successful_urls.txt", "a") as surls:
        surls.write(("\n".join(urls) + "\n").encode("utf-8"))


def process_url_group(group, useragentstr):
    rawDatas, successfull_urls = group.download(useragentstr, is_valid)
    save_count(successfull_urls)
    return extract_next_links(rawDatas), rawDatas


#######################################################################################


'''
STUB FUNCTIONS TO BE FILLED OUT BY THE STUDENT.
'''


def extract_next_links(rawDatas):
    outputLinks = list()
    '''
    rawDatas is a list of objs -> [raw_content_obj1, raw_content_obj2, ....]
    Each obj is of type UrlResponse  declared at L28-42 datamodel/search/datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded.
    The frontier takes care of that.

    Suggested library: lxml
    '''
    '''self.url = url
        self.content = content
        self.error_message = error_message
        self.headers = headers
        self.http_code = http_code
        self.is_redirected = is_redirected
        self.final_url = final_url

        # Things that have to be set later by crawlers
        self.bad_url = False
        self.out_links = set()'''
    global most_outlinks_page
    global subdomain_map
    for seed in rawDatas:
        if seed.error_message != '':
            seed.bad_url = True
            continue
        #print 'Error message: ' + seed.error_message + ' '
        '''Forbidden'''
        # print 'Headers: ' + str(seed.headers)
        '''
        Headers: {'Last-Modified': 'Tue, 19 Jul 2016 02:29:56 GMT', 'Content-Length': '309', 'ETag': '"66bb1ac-135-537f3dc4a7100"', 'Date': 'Wed, 08 Feb 2017 03:30:23 GMT', 'Accept-Ranges': 'bytes', 'Content-Type': 'text/html; charset=UTF-8', 'Server': 'Apache/2.2.15 (CentOS)'}
'''
        # print 'Http_code: ' + str(seed.http_code)
        '''200'''
        # print 'Is_redirected: ' + str(seed.is_redirected)
        if seed.is_redirected == True:
            base_href = seed.final_url
            # print 'Final_url: ' + seed.final_url
        else:
            base_href = seed.url

        parsed_url = urlparse(base_href)
        # fixing base_hrefs
        if parsed_url.path and parsed_url.path.find('.') == -1 and parsed_url.path[-1] != '/':
            base_href += '/'
        if (parsed_url.scheme == ''):
            base_href = "http://" + base_href
        
        subdomain_map[parsed_url.netloc] += 1
        '''parsed_url_domain = parsed_url.netloc.split('.')
        netloc_length = len(parsed_url_domain)
        subdomain = ''
        if netloc_length > 3:
            if parsed_url_domain[0] == 'www':
                for index in range(1, netloc_length - 3):
                    subdomain = subdomain + parsed_url_domain[index] + '.'
                subdomain = subdomain + parsed_url_domain[netloc_length - 2]
                subdomain_map[subdomain] += 1
        elif netloc_length == 3:
            if parsed_url_domain[0] != 'www':
                subdomain_map[parsed_url_domain[0]] += 1'''
        
        page = BeautifulSoup(seed.content, "lxml")
        
        num_of_outlinks = 0
        
        for link in page.find_all("a", href=True):
            num_of_outlinks += 1
            url = urljoin(base_href, link['href']);
            seed.out_links.add(url)
            outputLinks.append(url)
        for key in most_outlinks_page:
            if num_of_outlinks > key:
                most_outlinks_page = {num_of_outlinks:set([base_href])}
            elif num_of_outlinks == key:
                most_outlinks_page[key].add(base_href)
    # print outputLinks
    return outputLinks


def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be downloaded or not.
    Robot rules and duplication rules are checked separately.

    This is a great place to filter out crawler traps.
    '''
    global num_of_invalids
    parsed = urlparse(url)
  
    '''ParseResult(scheme=' ', netlock=' ', path='', params='', query='',
     fragment='' '''
    # dynamic page traps
    if "wics.ics.uci.edu/events" in url or \
                    "archive.ics.uci.edu/ml" in url or \
                    "ics.uci.edu/~eppstein/pix" in url or \
                    "calendar.ics.uci.edu" in url or \
                    "kdd.ics.uci.edu" in url or \
                    "ics.uci.edu/~mlearn" in url or \
                    "ganglia.ics.uci.edu" in url or \
                    "ics.uci.edu/~agelfand" in url:
        #print 'trapsss'
        num_of_invalids += 1

        return False

    # print 'not trapped'

    if parsed.scheme not in set(["http", "https"]):
        num_of_invalids += 1
        return False
    # print 'has http scheme'
    # pathological and path-depth traps
    urlpath = parsed.path.split('/')  # urlpath is now a list
    # path-depth 20
    if len(urlpath) >= 22:
        num_of_invalids += 1
        return False
    # print 'not super deep'
    # repetition of 3
    for i in range(2, len(urlpath)):
        pattern = urlpath[i]
        if pattern == urlpath[i - 1] and pattern == urlpath[i - 2]:
            num_of_invalids += 1
            return False
    # print 'not repeating paths'
    # fight malformed paths
    # shitty php
    urlpath = parsed.path  # urlpath is now a str
    if len(urlpath.split('.php')) >= 3:
        num_of_invalids += 1
        return False
        # general form
    # print 'there is no multiple phps'

    dot = urlpath.find('.')
    if dot != -1 and urlpath.find('/', dot) != -1:
        num_of_invalids += 1
        return False
    # print 'does not bad url path append'

    if parsed.hostname[-1] == '.':
        num_of_invalids += 1
        return False
    # print 'hostname is legit'
    try:
        if ".ics.uci.edu" in parsed.hostname \
               and not re.search(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                 + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                 + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                 + "|thmx|mso|arff|rtf|jar|csv" \
                                 + "|rm|smil|wmv|swf|wma|zip|rar|gz|tsv)$", parsed.path.lower()):

            return True
        else:
            num_of_invalids += 1
            return False

    except TypeError:
        print ("TypeError for ", parsed)
