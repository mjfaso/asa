import datetime
import io
import json
import ipdb
import pandas as pd
import logging
import os
import requests
import string
from datetime import datetime, timedelta, timezone
from dateutil import relativedelta
from pandas.io.json import json_normalize

class Client():
    __base_url__ = 'https://api.searchads.apple.com/api/{}'
    # TEMP stuff while redoing paths for the data team
    __work__ = 'work'

    # Edit this if different on your PC
    __data_analysis_loc__ = 'filestream'

    # Name of search_ads dir in our paths
    __search_ads__ = 'search_ads'

    __home_dir__ = os.path.expanduser('~')
    # DATA_ANALYSIS_DIR = os.path.join(HOME_DIR, DATA_ANALYSIS_LOC)
    __data_analysis_dir__ = os.path.join(__home_dir__, __data_analysis_loc__)
    # DATA_DIR = create_dir(os.path.join(DATA_ANALYSIS_DIR, 'data/clients/{}/{}/'.format(client.lower(), SEARCH_ADS)))
    __key_dir__ = os.path.join(__data_analysis_dir__, 'keys', 'search_ads')

    __orgs_filename__ = 'orgs.json'
    
    _campaigns_metadata = [
        ['metadata', 'campaignId'],
        ['metadata', 'campaignName'],
        ['metadata', 'deleted'],
        ['metadata', 'campaignStatus'],
        ['metadata', 'app', 'appName'],
        ['metadata', 'app', 'adamId'],
        ['metadata', 'servingStatus'],
        ['metadata', 'servingStateReasons'],
        ['metadata', 'countriesOrRegions'],
        ['metadata', 'modificationTime'],
        ['metadata', 'totalBudget', 'amount'],
        ['metadata', 'totalBudget', 'currency'],
        ['metadata', 'dailyBudget', 'amount'],
        ['metadata', 'dailyBudget', 'currency'],
        ['metadata', 'displayStatus'],
        ['metadata', 'orgId'],
        ['metadata', 'countryOrRegionServingStateReasons'],
    ]
    
    _adgroups_metadata = [
        ['metadata', 'adGroupId'],
        ['metadata', 'adGroupName'],
        ['metadata', 'startTime'],
        ['metadata', 'endTime'],
        ['metadata', 'cpaGoal'],
        ['metadata', 'defaultCpcBid', 'amount'],
        ['metadata', 'defaultCpcBid', 'currency'],
        ['metadata', 'deleted'],
        ['metadata', 'adGroupStatus'],
        ['metadata', 'adGroupServingStatus'],
        ['metadata', 'adGroupServingStateReasons'],
        ['metadata', 'modificationTime'],
        ['metadata', 'adGroupDisplayStatus'],
        ['metadata', 'automatedKeywordsOptIn'],
    ]
    
    _keywords_metadata = [
        ['metadata', 'keywordId'],
        ['metadata', 'keyword'],
        ['metadata', 'keywordStatus'],
        ['metadata', 'matchType'],
        ['metadata', 'bidAmount', 'amount'],
        ['metadata', 'bidAmount', 'currency'],
        ['metadata', 'deleted'],
        ['metadata', 'keywordDisplayStatus'],
        ['metadata', 'adGroupId'],
        ['metadata', 'adGroupName'],
        ['metadata', 'adGroupDeleted'],
        ['metadata', 'modificationTime'],
    ]
    
    _search_terms_metadata = [
        ['metadata', 'keywordId'],
        ['metadata', 'keyword'],
        ['metadata', 'matchType'],
        ['metadata', 'bidAmount', 'amount'],
        ['metadata', 'bidAmount', 'currency'],
        ['metadata', 'deleted'],
        ['metadata', 'keywordDisplayStatus'],
        ['metadata', 'adGroupId'],
        ['metadata', 'adGroupName'],
        ['metadata', 'adGroupDeleted'],
        ['metadata', 'searchTermText'],
        ['metadata', 'searchTermSource'],
    ]
    
    def __init__(self, name, api_version="v2", orgs_filename="orgs.json"):
        # the client name
        self.name = name
        # needed for search ads auth
        self.cert = self._get_cert()
        self.pem = self._get_pem()
        # orgs are the possible groups within a search ads account
        self.orgs = self.get_org_ids()
        # paths
        self.__base_url__ = self.__base_url__.format(api_version)
        self.key_dir = os.path.join(os.path.expanduser("~"), "keys", "search_ads")
        
    # Return filepath for the orgs file
    def get_org_filepath(self, client_name):
        """
        Returns filepath for the orgs.json file which contains the orgId for each
        client used in the Apple Search Ads API. Doesn't check if file exists.
        """
        filepath = os.path.join(self.__key_dir__, self.__orgs_filename__)
        return filepath

    def _get_cert(self):
        """
        Returns filepath for the cert file which contains the cert for each
        client used in the Apple Search Ads API. Doesn't check if file exists.
        """
        filepath = os.path.join(self.__key_dir__, '{}.pem'.format(self.name.lower()))
        return filepath

    def _get_pem(self):
        """
        Returns filepath for the key file which contains the key for each
        client used in the Apple Search Ads API. Doesn't check if file exists.
        """
        filepath = os.path.join(self.__key_dir__, '{}.key'.format(self.name.lower()))
        return filepath

    def get_org_ids(self):
        """
        Returns dict with storefronts as keys and orgIds as values for the Apple Search Ads
        API.
        If file is not found, returns None
        """
        filepath = self.get_org_filepath(self.name)
        try:
            with open(filepath, 'r') as orgs_file:
                orgs = json.load(orgs_file)
        except FileNotFoundError:
            logging.warning("The orgs file is not at {}. Double check the path and name.".format(filepath))
            return []
        except KeyError:
            logging.warning("There is no org specified for {} at {}. Open file and double check.".format(self.name,filepath))
            return []
        # if no exception was raised
        else:
            try:
                return list(orgs[self.name].values())
            except KeyError:
                logging.warning("There is no org specified for {}. Open file and double check.".format(self.name))
                return []
            
    def _get_headers(self, org_id):
        """
        Returns the mandatory headers to include in every Apple Search Ads API call
        """
        return {
            # org_id must be passed in header for all requests
            'Authorization': 'orgId={}'.format(org_id),
            'Content-Type': 'application/json',
        }
    
    def _get_data(self, url, df, org_id=None, query=None):
        responses = []
        ipdb.set_trace()
        # get data for all orgs if none is passed
        orgs = self.orgs if not org_id else [org_id]

        for org in orgs:
            offset = 0
            total_results = 1000
            while offset < total_results:
                
                params = {
                    "limit":1000,
                    "offset": offset
                }
                # the search endpoints take in a query
                if query:
                    params["query"] = query

                r = requests.get(
                    url,
                    cert = (self.cert, self.pem),
                    headers = self._get_headers(org),
                    params = params
                )
                
                # if the request is valid, there might be more results
                # which are not returned because there is a 1000 result
                # limit on the api
                if r.status_code == 200:                
                    responses.append(r)
                    #start_index = r.json()["pagination"]["startIndex"]
                    #items_per_page = r.json()["pagination"]["itemsPerPage"]
                    total_results = r.json()["pagination"]["totalResults"]
                    offset += 1000
                # if the request is invalid, exit loop
                else:
                    break
         
        if not df:
            return responses
            
        # all the responses are invalid
        if not responses:
            if df:
                return pd.DataFrame()
            else:
                return []
        # some responses are valid and we want to return a dataframe
        elif df:
            return self._convert_to_df(responses)
        # responses are valid and we want the responses
        else:
            return responses
          
    def _convert_to_df(self, responses):
        all_df = []
        for response in responses:
            df = json_normalize(response.json()['data'])
            all_df.append(df)
        final_df = pd.concat(all_df, ignore_index=True, sort=True)
        return final_df
    
    def get_campaigns(self, org_id=None, df=True):
        # URL
        url = "{}/campaigns".format(self.__base_url__)
        
        return self._get_data(url, df, org_id=org_id)
    
    def get_campaign_ids(self, org_id=None, status=None):

        df = self.get_campaigns()

        if type(df) == list and df == []:
            return []
        
        if not df.empty:
            if org_id:            
                # org_id
                df = df[(df["orgId"] == int(org_id))]
                # org_id and status
                if status:
                    df = df[df["status"] == status] 
            # just status
            elif status:
                df = df[df["status"] == status]
            return list(zip(df["id"], df["name"], df["orgId"]))
        else:
            return []
        
    def get_adgroup_ids(self, org_id=None, status="ENABLED"):
        campaigns = self.get_campaign_ids(org_id)
        adgroups_df = []

        for campaign in campaigns:
            campaign_id, campaign_name, org_id = campaign
            df = self.get_adgroups(campaign_id, org_id=org_id)
            if not df.empty:
                adgroups_df.append(df)
        if not adgroups_df:
            return adgroups_df
        else:
            adgroups_df = pd.concat(adgroups_df, ignore_index=True, sort=True)
            return list(zip(list(adgroups_df["orgId"]), list(adgroups_df["campaignId"]), list(adgroups_df["id"]), list(adgroups_df["name"])))
    
    def get_adgroups(self, campaign_id, org_id=None, df=True):
        # URL
        url = "{}/campaigns/{}/adgroups".format(self.__base_url__, campaign_id)
        
        return self._get_data(url, df, org_id=org_id)
    
    def get_all_adgroups(self, org_id=None):
        campaigns = self.get_campaign_ids(org_id)
        
        all_adgroups = []
        for campaign in campaigns:
            campaign_id, name = campaign
            adgroup_df = self.get_adgroups(campaign_id, org_id=org_id)
            if not adgroup_df.empty:
                all_adgroups.append(adgroup_df)
        if not all_adgroups:
            return all_adgroups
        else:
            all_adgroups = pd.concat(all_adgroups, ignore_index=True, sort=True)
            return all_adgroups

    def get_keywords(self, campaign_id, adgroup_id, org_id=None, df=True):
        # URL
        url = "{}/campaigns/{}/adgroups/{}/targetingkeywords".format(
            self.__base_url__, campaign_id, adgroup_id,
        )
        
        return self._get_data(url, df, org_id=org_id)
    
    def get_all_keywords(self, df=True, org_id=None):
        all_keywords = []
        org_ids = self.orgs if not org_id else org_id
        for org in org_ids:
            adgroups = self.get_adgroup_ids(org_id=org)
            for adgroup in adgroups:
                _, campaign_id, adgroup_id, _ = adgroup
                
                keywords = self.get_keywords(campaign_id, adgroup_id, org_id=org, df=df)
                if df:
                    all_keywords.append(keywords)
                else:            
                    all_keywords.extend(keywords)
            if not all_keywords:
                return []
        if df and all_keywords:
            all_keywords_df = pd.concat(all_keywords, ignore_index=True, sort=True)
            return all_keywords_df
        elif df and not all_keywords:
            return pd.DataFrame()
        else:
            return all_keywords
    
    def get_negative_keywords(self, campaign_id, org_id=None, df=True):
        # URL
        url = "{}/campaigns/{}/negativekeywords".format(self.__base_url__, campaign_id)
        
        return self._get_data(url, df, org_id=org_id)
    
    def get_all_negative_keywords(self, org_id=None, df=True):
        all_neg_keywords = []
        org_ids = self.orgs if not org_id else org_id
        for org in org_ids:
            campaigns = self.get_campaign_ids(org_id=org)
            for campaign in campaigns:
                campaign_id, campaign_name, _ = campaign
                
                neg_keywords = self.get_negative_keywords(campaign_id, org_id=org, df=df)
                if df:
                    all_neg_keywords.append(neg_keywords)
                else:            
                    all_neg_keywords.extend(neg_keywords)
            if not all_neg_keywords:
                return []
        if df and all_neg_keywords:
            all_neg_keywords_df = pd.concat(all_neg_keywords, ignore_index=True, sort=True)
            return all_neg_keywords_df
        elif df and not all_neg_keywords:
            return pd.DataFrame()
        else:
            return all_neg_keywords