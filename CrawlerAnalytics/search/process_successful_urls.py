from urlparse import urlparse
from collections import Counter
total_urls = set()
subdomains = Counter()
with open('successful_urls.txt', 'r') as su1, open('successful_urls_2.txt', 'r') as su2:
    for line1 in su1:
        total_urls.add(line1.strip())
    for line2 in su2:
        total_urls.add(line2.strip())

for i in total_urls:
    parsed = urlparse(i)
    
    subdomains[parsed.netloc] += 1

    

with open('combinedAnalytics.txt','w') as f:
    f.write('Found unique links: ' + str(len(total_urls)))
    f.write('\n\n')
    f.write('Links in each subdomain \n')
    for i in subdomains.most_common():
        f.write(str(i) + '\n')