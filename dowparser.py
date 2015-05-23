import traceback
import re
from bs4 import BeautifulSoup

ASPX_PATT = re.compile('"/?([^ ]+?(?:\.aspx).*?)"', re.I)

# Replace absolute paths with relative ones for resources
def replaceAbsoluteFunc(m):
    return m.group(1) + m.group(2)

REPLACE_ABSOLUTE_PATT = re.compile('(")\/?([^\s]+?(?:(?:\.css)|(?:\.png)|(?:\.gif)|(?:\.jpeg)|(?:\.jpg))")', re.I)
def replaceAbsolute(cont):
    ''' Replace absolute resource paths with relative ones, so the website could be viewed offline '''
    return REPLACE_ABSOLUTE_PATT.sub(replaceAbsoluteFunc, cont)

def getPageUrls(cont):
    ''' Get the search page urls (1-2-...whatever) '''
    print 'Finding urls'
    soup = BeautifulSoup(cont)
    pages_span = soup.find(id='divResultsTable').table.tr.span
    
    if pages_span == None:
        return []
    pages = pages_span.find_all('a')
    
    page_urls = []
    for page in pages:
        page_urls.append(page['href'])
        
    return page_urls    
    
def getRecordData(cont):
    ''' Fetch the id of the record and the relevant html elements and return them. '''
    # If needed, replace absolute links.
    #cont = replaceAbsolute(cont)
    soup = BeautifulSoup(cont)
    
    #id = soup.find(id='lblProfileIdText').text
    
    elem = soup.find(id='divRecordView')
    if elem == None:
        elem = soup.find(id='divRecordViewCommon')
    
    data = ''
    elems = elem.parent.find_all('div', recursive=False)
    
    for e in elems:
        data += e.prettify()
        
    return data

    # A language option that's in the name hover function, maybe useful later
RESULT_LANGUAGE = { '1': 'English',
                    '2': 'Spanish',
                    '4': 'French',
                    '5': 'German',
                    '6': 'Russian',
                    '7': 'Japanese',
                    '8': 'Polish',
                    '9': 'Traditional Chinese',
                    '10': 'Simple Chinese' }

WORD_PATT = re.compile('([\w]+)')
HOVER_TEXT_PATT = re.compile('ShowHoverText\(event,(.+?)\);')

def getText(propertyName):
    
    def __getText(soup):
        return {propertyName: ' '.join(WORD_PATT.findall(soup.get_text()))}
    
    return __getText

TITLE_PATT = re.compile('title=\"(.+?)\"/>')
def parseAttr(propertyName):
    
    # Parse the attributes associated with 
    def __parseAttr(soup):
        # This isn't working, doing it myself
        title = TITLE_PATT.findall(str(soup))
        if len(title) == 0:
            return {propertyName: ''}
        else:
            return {propertyName: title[0]}
        
    return __parseAttr
    
def parseName(soup):
    # The name has more data, 
    link = soup.a
    try:
        hover_data = HOVER_TEXT_PATT.findall(link['onmouseover'])[0].split(',')        
    except Exception, e:
        print traceback.format_exc()
    
    result = dict(zip(('dow_id', 'name', 'gender', 'dob', 'match_type', 'variation', 'language'), hover_data))
    result['url'] = link['href']
    
    return result

def parsePages(pages):
    
    results = []
    for page in pages:
        results.extend(parseNormal(page))
    return results

# A dictionary for parsing people queries
# Each index in the html points to it's name and processing function
NORMAL_HANDLERS = { 0: parseAttr('attr1'), # Those 3 are attributes of the entity.. not sure each one means
                    1: parseAttr('attr2'),
                    2: parseAttr('attr3'),
                    3: parseName,
                    4: getText('country'),
                    5: getText('title'),
                    6: getText('subsidiary'),
                    7: getText('match') }
def parseNormal(cont):
    print 'Parsing page'
    soup = BeautifulSoup(cont)
    results = []
    
    matches = soup.find('table', id='grdSearch').find_all('tr')[1:-1]
    for match in matches:
        result_data = {}
        params = match.find_all('td')
        for i in xrange(len(params)):
            if NORMAL_HANDLERS.has_key(i):
                result_data.update(NORMAL_HANDLERS[i](params[i]))
        results.append(result_data)
        
    return results

"""
def getSearchResults(cont):
    ''' Get the html of the search results '''
    result = ''
    cont = replaceAbsolute(cont)
    soup = BeautifulSoup(cont)
    matches = soup.find(id='grdSearch').find_all('tr')[1:-1]    
    for match in matches:
        result += match.prettify()
        
    return result

def getPageUrls(cont):
    ''' Get the search page urls (1-2-...whatever) '''
    print 'Finding urls'
    soup = BeautifulSoup(cont)
    pages = soup.find(id='divResultsTable').table.tr.span.find_all('a')
    
    page_urls = []
    for page in pages:
        page_urls.append(page['href'])
        
    return page_urls

def getRecordUrls(cont):
    ''' Get the records needed for this page to function offline '''
    result = ASPX_PATT.findall(cont)
    return result

# A dictionary for parsing people search records
RELATIVES_HANDLERS = { 0: parseAttr('attr1'), # Those 3 are attributes of the entity.. not sure each one means
                       1: parseAttr('attr2'),
                       2: parseAttr('attr3'),
                       3: getText('name'),
                       4: getText('type'),
                       5: getText('relation'),}    
def parseRecord(soup):
    '''
    NOT FINISHED
    id='TRDeceased'    
    '''
    # Watchlist
    soup.find(id='divRecordViewWatchlist')
    
    # Relatives and associates
    # Use code from parse normal somehow
    soup.find(id='grdCommonRCA')
    
    # Watchlist status
    soup.find(id='lblWLStatus')
    soup.find(id='lblWLStatusText')
    
    # Deceased
    soup.find(id='TRDeceased').find(class_='Values').text
    
    # Categories
    soup.find(id='lblCategory1_1')
    soup.find(id='lblCat2_2')
    
    # Anti corruption (like watchlist for companies)
    soup.find(id='divRecordViewAC')

# Replace absolute paths with relative ones for resources
def replaceAbsoluteFunc(m):
    return m.group(1) + m.group(2)
# TODO: remove trailing slash
REPLACE_ABSOLUTE_PATT = re.compile('(")\/?([^\s]+?(?:(?:\.css)|(?:\.png)|(?:\.gif)|(?:\.jpeg)|(?:\.jpg))")')

def replaceAbsolute(cont):
    return REPLACE_ABSOLUTE_PATT.sub(replaceAbsoluteFunc, cont)

RESOURCE_PATT = re.compile('"([^\s]+?(?:(?:\.css)|(?:\.png)|(?:\.gif)|(?:\.jpeg)|(?:\.jpg)))"')
ASPX_PATT = re.compile('"/?([^ ]+?(?:\.aspx).*?)"', re.I)
def handleContent(cont, relevant_id):
    '''
    NOT FINISHED
    Handle the content and return all relevant dependencies and resources
    '''
    # Replace the absolute resources
    cont = replaceAbsolute(cont)
    
    resources = set(RESOURCE_PATT.findall(cont))
    
    soup = BeautifulSoup(cont)
    # Get the urls from the relevant id only
    records = set(aspx_patt.findall(str(soup.find(relevant_id))))
    
    # return both of them
    return cont, resources, records
"""