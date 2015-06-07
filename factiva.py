import requests
import re
from bs4 import BeautifulSoup
import json
from datetime import datetime
import logging
from ConfigParser import SafeConfigParser
from Queue import Queue
import time

import requests.utils
import pprint

import pygres
import rabbitcoat
import dowparser
from helper import *

# Turn down requests and pika logging
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("pika").setLevel(logging.WARNING)

HTML_STRUCTURE = '<html><head><meta charset="UTF-8"></head><body>%s</body>'

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'}

# Need to add the txtName, hdnClientTime, hdnClientTimeLocal
DEFAULT_CRITERIA = {"AMCOperator": "rdoAMC10108",
 "CountryType": "rbCountryTypeOr",
 "HeightenedRisk": "rdoHR10108",
 "Oelviewtypes": "rdoOElClassicView",
 "Oolviewtypes": "rdoOOlClassicView",
 "OtherExclusionList": "rdoOE2",
 "OtherOfficialList": "rdoOO10108",
 "SanctionsList": "rdoSan10108",
 "btn10012": "Search\r\n",
 "chk10036": "on",
 "chkAC": "on",
 "chkIncludeUnknown": "on",
 "chkWL": "on",
 "ddlDates": "0",
 "ddlFromYear": "0",
 "ddlIDType": "1",
 "ddlOEDates": "0",
 "ddlOODates": "0",
 "ddlSOFilter": "0",
 "ddlToYear": "0",
 "hdnAMCText": "All Adverse Media Categories",
 "hdnAdvanceSearch": "",
 "hdnAdverseMediaCategory": "<a href=\"javascript:void(0)\" class=\"NavlinkMaroon\" id=\"AllAMCLists\" Name=\"AllAMCLists\" onclick=\"removeAMCLink(this,1)\">All Adverse Media Categories</a>",
 "hdnBlacklistTerms": "[^\\!\\*\\(\\)\\-\\=\\:\\?\\;\\[\\]\\_\\`\\.\\'\\,]",
 "hdnClientTime": "3-23-2015 19:46:54",
 "hdnClientTimeLocal": "3-23-2015 21:46:54",
 "hdnContentSetSelection": "1|1|0",
 "hdnCountExcludes": "0",
 "hdnCountries": "All Regions",
 "hdnCountriesText": "",
 "hdnCountryTypeOperator": "Or",
 "hdnCountryTypes": "All Country Types",
 "hdnCountryTypesText": "",
 "hdnEntity": "Entity Name ",
 "hdnFirstName": "First Name ",
 "hdnHeightenedRisk": "<a href=\"javascript:void(0)\" class=\"NavlinkMaroon\" id=\"AllHRLists\" Name=\"AllHRLists\" onclick=\"removeHRLink(this,1)\">All Special Interest Categories</a>",
 "hdnHeightenedRiskText": "",
 "hdnIsModify": "No",
 "hdnIsModifyCountryTypeFilter": "No",
 "hdnMatches": "0",
 "hdnMiddleName": "Middle Name ",
 "hdnModifySearch": "No",
 "hdnName": "Name ",
 "hdnOtherExclusion": "<a href=\"javascript:void(0)\" class=\"NavlinkMaroon\" id=\"AllOELists\" Name=\"AllOELists\" onclick=\"removeOELink(this,1)\">All Other Exclusion Lists</a>",
 "hdnOtherExclusionText": "",
 "hdnOtherOfficial": "<a href=\"javascript:void(0)\" class=\"NavlinkMaroon\" id=\"AllOOLists\" Name=\"AllOOLists\" onclick=\"removeOOLink(this,1)\">All Other Official Lists</a>",
 "hdnOtherOfficialText": "",
 "hdnOtherRoles": "False",
 "hdnPEP": "<a href=\"javascript:void(0)\" class=\"NavlinkMaroon\" id=\"AllCategories\" Name=\"AllCategories\" onclick=\"removePEPLink(this,1)\">All PEP Categories</a>",
 "hdnPEPCategories": "",
 "hdnPreciseBroad": "Near",
 "hdnPreviousRoles": "False",
 "hdnPriOccupation": "False",
 "hdnProfileID": "Value",
 "hdnSanctions": "<a href=\"javascript:void(0)\" class=\"NavlinkMaroon\" id=\"AllSanction\" Name=\"AllSanction\" onclick=\"removeSancLink(this,1)\">All Sanctions Lists</a>",
 "hdnSanctionsText": "",
 "hdnSearchParameter": "",
 "hdnSearchType": "simple",
 "hdnSearchTypeNB": "Near",
 "hdnSearchTypeTab": "",
 "hdnSurName": "Surname ", 
 "hdnTokenizedAlerts": "1|Are you sure you wish to continue?,2|\"From\"\" date should be earlier than \"\"To\"\" date.\",3|\"Please enter a \"\"From\"\" date and \"\"To\"\" date.\",4|A Risk Score has already been set for this Occupation Category. Adding this Occupation Category to your Search Exclusion list will delete this Risk Score and records from this Occupation Category will be excluded from users search results. Click OK to proceed with this change.,5|A Risk Score has already been set for this Region. Adding this Region to your Search Exclusion list will delete this Risk Score and records from this Region will be excluded from users search results. Click OK to proceed with this change.,6|Do you want to delete this saved News Filter?,7|No Occupation Categories have been excluded for this account,8|No Records found in Dow Jones Watchlist,9|No Regions have been excluded for this account,10|No results match your search criteria.,11|Please enter a 4 digit year format (YYYY).,12|Please enter a correctly formatted date (dd/mm/yyyy).,13|Please Enter a two digit in Name Space,14|Please enter a year from the past.,15|Please enter all login credentials to access Dow Jones Risk & Compliance,16|Please enter Password,17|Please enter search criteria to perform a close match search,18|Please enter User Id,19|Please enter valid username and password,20|Please include at least one criteria to run a News Search,21|Please include at least one Region to perform a close match search.,22|Please select at least one value from the Search Filters.,23|Please modify your search to include at least one search term.,24|Please select a Region,25|Please select a Risk Score,26|Please select an Occupation Category,27|Please select and enter search criteria for each required Name Type (select at least one).,28|Please Select Risk Score,29|Please submit a News Filter name.,30|Risk Score feature is disabled for this Account. Do You want to enable Risk Score?,31|You have entered an invalid date in the Enter Date Range option. Please re-enter your date values.,32|You have entered an invalid date in the Enter Date Range option. Please re-enter your month values.,33|You have selected date range in the Enter Date Range option. Please enter from date and to date.,34|Please select one or more documents.,35|Search using only first name is not allowed.Please enter surname.,36|At least one content set should be selected.,37|Searches using the following characters on their own are not allowed ! * ( ) - = : ? ; [ ] _ ` . ' ,,38|Searching on day only is not allowed.,39|Searching on day and year is not allowed.,40|Searching by strict date match is not allowed for an incomplete date or if a year range is selected.,41|Invalid date.,42|Please enter ID Number Value.,43|Searching on \"From\" year only is not allowed.,44|Searching on \"To\" year only is not allowed.,45|\"From\" year cannot be later than \"To\" year.,46|A minimum of three characters is required when * is used to truncate a name.,47|,", 
"hdnWL": "1",
 "hdnrdoAMC": "Or",
 "hdnrdoHR": "Or",
 "hdnrdoOE": "Or",
 "hdnrdoOO": "Or",
 "hdnrdoSanctions": "Or",
 "rbExcludeAMRCA": "0",
 "rbRecordType": "0",
 "rdNear": "rdNear",
 "slviewtypes": "rdoSanctionClassicView",
 "txtAMCListIDs": "",
 "txtCountry": "",
 "txtCountryCodes": "",
 "txtCountryCodesNo": "",
 "txtCountryIDs": "",
 "txtCountryIDsNo": "",
 "txtCountryTypeIDs": "",
 "txtDayFrom": "",
 "txtDayTo": "",
 "txtEntityName": "",
 "txtFirstName": "",
 "txtHRListIDs": "",
 "txtMiddleName": "",
 "txtMonthFrom": "",
 "txtMonthTo": "",
 "txtName": "",
 "txtNotAMCListIDs": "",
 "txtNotHRListIDs": "",
 "txtNotOEListIDs": "",
 "txtNotOOListIDs": "",
 "txtNotPEPCategoryIDs": "",
 "txtNotSanctionRefIDs": "",
 "txtNotSanctionRefIDs_New": "",
 "txtOEDayFrom": "",
 "txtOEDayTo": "",
 "txtOEListIDs": "",
 "txtOEMonthFrom": "",
 "txtOEMonthTo": "",
 "txtOEYearFrom": "",
 "txtOEYearTo": "",
 "txtOODayFrom": "",
 "txtOODayTo": "",
 "txtOOListIDs": "",
 "txtOOMonthFrom": "",
 "txtOOMonthTo": "",
 "txtOOYearFrom": "",
 "txtOOYearTo": "",
 "txtPEPCategoryIDs": "",
 "txtProfileId": "",
 "txtSanctionRefIDs": "",
 "txtSanctionRefIDs_New": "",
 "txtSurName": "",
 "txtYearFrom": "",
 "txtYearTo": ""}

def dateToStr(d):
    ''' This is how factiva represents dates. '''
    return '%s-%s-%s %s:%s:%s' %(d.month, d.day, d.year, d.hour, d.minute, d.second)

def copyForm(form):
    data = {}
    inputs = form.find_all('input')
    for input in inputs:
        if input['type'] == 'hidden' and input.has_attr('value'):
            data[input['name']] = input['value']
    
    return data

MAIN_URL = 'https://djrc.dowjones.com/'
SEARCH_MAIN_ID = 'grdSearch'
SOURCE_FORMAT = 'factiva'

OUTPUT_DIR = 'output/'

DEFAULT_CONFIG = 'settings.ini'

FACTIVA_SECTION = 'FACTIVA'

LOGIN_INTERVAL = 15*60

# Can inherit Thread if needed
class DowJones(object):
    
    def __init__(self, config='conf/factiva.conf', rabbit_config='conf/rabbitcoat.conf', pygres_config='conf/pygres.conf'):
        self.logger = getLogger('factiva')
        self.logger.info('Initializing factiva')
        
        self.db_articles = pygres.PostgresArticles(self.logger, pygres_config)
        
        self.__loadConfig(config)
        
        self.s = requests.session()
        
        self.s.headers = HEADERS
        
        self.queries = Queue()
        self.sender = self.sender = rabbitcoat.RabbitSender(self.logger, rabbit_config, self.out_queue)
        
        self.last_login = 0
        # Give it some time to get the lock
        time.sleep(2)
        
        self.receiver = rabbitcoat.RabbitReceiver(self.logger, rabbit_config, self.in_queue, self.__rabbitCallback)
        self.receiver.start()
    
    def __loadConfig(self, config):
        parser = SafeConfigParser()
        parser.read(config)
        self.username = parser.get(FACTIVA_SECTION, 'username')
        self.password = parser.get(FACTIVA_SECTION, 'password')
        self.in_queue = parser.get(FACTIVA_SECTION, 'in_queue')
        self.out_queue = parser.get(FACTIVA_SECTION, 'out_queue')
    
    def __login(self):
        ''' Login into dowjones '''
        if time.time() - self.last_login < LOGIN_INTERVAL:
            return
        
        self.last_login = time.time()
        
        self.logger.info('Logging into dowjones...')
        
        login_url = 'https://djlogin.dowjones.com/login.asp?productname=rnc'
        
        data = {
            'LoginEmailId': self.username,
            'UserPassword': self.password
        }
        res = self.s.post(login_url, data)
        
        self.__browserLogin()
    
    def __browserLogin(self):
        ''' Perform a different login to imitate browser behavior '''
        self.logger.info('Imitating browser behavior...')
        browser_login = 'http://djlogin.dowjones.com/rnc/default.aspx'
        
        res = self.s.get(browser_login)
        
        soup = BeautifulSoup(res.text)
        
        ref = soup.form['action']
        data = copyForm(soup.form)
        
        res = self.s.post(ref, data)
    
    def __getRecords(self, query, records):
        results = [] 
        for record in records:
            record_url = record['url']
            
            self.logger.debug('Getting  record %s' %record_url)
            res = self.s.get(MAIN_URL + record_url)
            
            if res.status_code != 200:
                self.logger.error('Failed to get record %s' %record)
            
            # Find the record data
            data = dowparser.getRecordData(res.text)
            
            id = self.db_articles.AddArticle(data, SearchEngines.FACTIVA)
            #id = 1
            
            # url is irrelevant here, since it changes constantly.
            results.append({ SOURCE_KEY: SOURCE_FORMAT,
                             ID_KEY: id,
                             QUERY_KEY: query,
                             TITLE_KEY: record['name'],})
        
        return results

    def __query(self, query):
        ''' Search dow jones and get the relevant records '''
        
        self.__login()
        
        search_url = 'searchcriteria.aspx'
        
        # Query for page specific parameters(like event validation and stuff)
        param_res = self.s.get(MAIN_URL + search_url)
        soup = BeautifulSoup(param_res.text)
        
        # Copy defaults, and set relevant search parameters
        data = DEFAULT_CRITERIA.copy()
        data.update(copyForm(soup.form))
        
        data['txtName'] = query
        data['hdnClientTime'] = dateToStr(datetime.utcnow())
        data['hdnClientTimeLocal'] = dateToStr(datetime.now())
        
        # Query the first page and save the result
        res = self.s.post(MAIN_URL + search_url, data)
        
        page_urls = dowparser.getPageUrls(res.text)
        # No results
        if len(page_urls) == 0:
            return []
        
        pages = []
        pages.append(res.text)
        # Skip the first page since we got it already
        for url in page_urls[1:]:
            res = self.s.get(MAIN_URL + url)
            if res.status_code!=200:
                self.logger.error('Failed to get search page %s')
            
            # Add the search results to the main content
            pages.append(res.text)
        
        records = dowparser.parsePages(pages)
        results = self.__getRecords(query, records)
        
        return results
        
    def __sendResults(self, results, error=None):
        '''Sends the results to the next gear.'''
        if error:
            results = {ERROR_KEY: error}
        else:
            results = {RESULTS_KEY: results}
            
        self.sender.Send(results, corr_id = self.corr_id)
    
    def __rabbitCallback(self, data, properties):
        name = data[NAME_PARAM]
        # Ignore the original name for now, since dowjones seems to translate it to English anyway.
        origin_name = data.get(ORIGIN_NAME_PARAM)
        
        self.queries.put((name, properties.correlation_id))        
    
    def run(self):
        self.logger.info('Starting main query loop')
        
        while True:
            # Since only one query is running at a time, self.corr_id is fine
            query, self.corr_id = self.queries.get()
            self.logger.info('Starting query %s' %query)
            
            try:
                results = self.__query(query)
            except Exception:
                self.logger.exception('Exception in run')
                self.__sendResults(None, error='Exception while fetching results')
            
            self.logger.info('Sending query results %s to %s' %(query, self.out_queue))
            #TODO: Remove this
            self.__sendResults(results)

def main():
    jones = DowJones()
    jones.run()
    
if __name__ == '__main__':
    main()
