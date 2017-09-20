from nltk import word_tokenize
from bs4 import BeautifulSoup
import pickle #https://docs.python.org/2.7/library/pickle.html#pickle-protocols
import json
from collections import OrderedDict, Counter
import time
import sys
import math
import os.path
import re
from os import listdir
from operator import itemgetter
'''
Option: Instead of using json.load to make bookkeeping, what about going through
the directory WEBPAGES_SIMPLE and through the directories holding the docs?
'''

def doc_id_map(bookkeeping_map):
    '''
    bookkeeping_map should be in the form:
    dir/file -> url, where dir and file are numbers in string form and separated by "/"
    Expects that each directory has a maximum number of 500 documents.
    Otherwise, this doc_id generator would have to be modified
    '''
    doc_id_dict = {}
    '''
    doc_id_dict will map doc_id's generated in this function to bookkeeping_map's keys
    doc_ids should be in order relative to directory number and file/doc number
    i.e 0 -> 0/0; 1 -> 0/1; 15 -> 0/15; 500 -> 1/0; 515 -> 1/15
    '''
    for key in bookkeeping_map:

        directory, document = key.split('/')
        doc_id = 500*int(directory) + int(document)
        if doc_id == 50:
            print key
        doc_id_dict[doc_id] = key
    return OrderedDict(sorted(doc_id_dict.items(), key=itemgetter(0)))

def tokenize(text):
    content = []
    regex = re.compile('[\w]+', re.UNICODE)
    text = text.strip().lower()
    text = text.replace("_", " ")
    text = regex.findall(text)
    for i in text:
        if i.isalnum():
            content.append(i)
    return content


##        count = 0
##        doc_id_dict = {}
##        for d in listdir(os.path.abspath("WEBPAGES_SIMPLE")):
##                if os.path.isdir(d):
##                        for f in listdir(os.path.abspath(d)):
##                                doc_id_dict[count] = d+"/"+f
##        return doc_id_dict

with open('WEBPAGES_SIMPLE/bookkeeping.json') as corpuses:

    bookkeeping = json.load(corpuses)

##sys.exit()

index = OrderedDict()
special_index = OrderedDict()
'''
Follows model:
{term->[{doc_id->tf_doc_id}]}
'''

score_index = OrderedDict()
'''
Maps term to {doc_id->tfidf score}
'''

document_id_map = doc_id_map(bookkeeping)

start = time.time()

for doc_id in document_id_map.values():
    try:
        with open('WEBPAGES_SIMPLE/'+doc_id, 'r') as document:
            tokens = []
            special_tokens = []
            tag_soup = document.read()
            soup = BeautifulSoup(tag_soup, 'lxml')
            try:
                tokens.extend(tokenize(soup.get_text()))
                word_counter = Counter(tokens)
                for word in word_counter:
                    if word in index:
                        index[word][doc_id] = [ word_counter[word] ]
                    else:
                        index[word] = OrderedDict({doc_id: [ word_counter[word] ]})

                for special_tags in soup.find_all(['h1','h2','h3', 'b', 'strong', 'em']):
                    special_tokens.extend(tokenize(special_tags.get_text()))
                word_counter = Counter(special_tokens)
                for word in word_counter:
                    if word in special_index:
                        special_index[word][doc_id] = [ word_counter[word] ]
                    else:
                        special_index[word] = OrderedDict({doc_id: [ word_counter[word] ] })

            except AttributeError:

                print doc_id, 'is an empty file'
            except TypeError:
                print doc_id, 'encountered Type Error'

    except IOError:

        print doc_id, 'is missing from corpus'
print 'number of unique words', str(len(index))

print 'number of documents', str(len(bookkeeping))

with open('paulpleasegoogleit.pickle','wb') as index_pickle:
    pickle.dump(index, index_pickle)
with open('special_index.pickle', 'wb') as special_pickle:
    pickle.dump(special_index, special_pickle)
end = time.time()

print 'done in', str(end - start)

def tfidf(inverted_index, collection_size):
    '''
    Void function that modifies the inverted_index passed to this function.
    This function appends the tfidf score to the list of metadata associated with each document associated with each term
    i.e. term->doc_id->list of metadata, where index 0=tf, and index 1=tfidf
    '''
    for term in inverted_index:
        for doc_id in inverted_index[term]:
            '''
            inverted_index[term][doc_id] = term frequency in doc id
            '''
            tfidf_td = (1 + math.log10(inverted_index[term][doc_id][0])) * math.log10(collection_size / len(inverted_index[term]))
            inverted_index[term][doc_id].append(tfidf_td)

def tfidf_sp(inverted_index, collection_size):
    '''
    Void function that modifies the inverted_index passed to this function.
    This function appends the tfidf score to the list of metadata associated with each document associated with each term
    i.e. term->doc_id->list of metadata, where index 0=tf, and index 1=tfidf
    '''
    for term in inverted_index:
        for doc_id in inverted_index[term]:
            '''
            inverted_index[term][doc_id] = term frequency in doc id
            '''
            #equation must be scaled by a factor
            ##                        tfidf_td = (1 + math.log10(inverted_index[term][doc_id][0])) * math.log10(collection_size / len(inverted_index[term]))
            inverted_index[term][doc_id].append(tfidf_td)

def query_result(query, inverted_index, bookkeeping_map):
    '''
    Void function that sums all the tfidf scores for documents that contain at least 1 term in the query.
    Prints out the top 10 urls that have the highest summed tfidf scores
    '''
    ranked_docs = OrderedDict()
    '''
    Maps doc_id to score
    '''

    for term in query.split():
        if inverted_index.has_key(term):
            for doc_id in inverted_index[term]:
                if doc_id in ranked_docs:
                    ranked_docs[doc_id] += inverted_index[term][doc_id][1]
                else:
                    ranked_docs[doc_id] = inverted_index[term][doc_id][1]
        if special_index.has_key(term):
            for doc_id in special_index[term]:
                if doc_id in ranked_docs and ranked_docs[doc_id]:
                    ranked_docs[doc_id] += inverted_index[term][doc_id][1] * 1.5
                else:
                    ranked_docs[doc_id] = inverted_index[term][doc_id][1] * 1.5

    if len(ranked_docs) != 0:
        ranked_docs = sorted(ranked_docs.items(), key = itemgetter(1), reverse = True)
        count_topfive = 0
        for document in ranked_docs:
            count_topfive += 1
            print (str(count_topten) + ") Score: " + str(document[1]) + "  " + bookkeeping_map[document[0]])
            if count_topfive == 5:
                break
    else:
        print ("No documents in our database contain what you're looking for.\nWe are sorry.\nWe have failed you.\nWe shall commit seppuku now.")

tfidf(index, len(bookkeeping))

query = ""
while(True):
    query = raw_input("\nEnter a query to search: ").lower()
    query_result(query, index, bookkeeping)
    query = raw_input("\nFinished? (Y/N): ").lower()
    if (query == "y"):
        break

#time to find code within <p></p> and the content after that
#print len(soup.p)
#print len(soup.p.next_sibling)

#how to print out bookkeeping
#for i in data:
#    print (i)
#    print data[i]

#extend() merges 2 lists to one list with all elements of both
#append() adds the exact element to one list


#metadata for extra credit
#cosine values and stopwords
