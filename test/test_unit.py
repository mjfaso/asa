import unittest
import pandas as pd

import search_ads_two as asa

# all tests must start with test!
class TestClient(unittest.TestCase):
    
    CLIENT_MULTIPLE_ORGS = "ebay"
    CLIENT_VALID_LITTLE_DATA = "device_magic"
    CLIENT_INVALID = "invalid_client"
    CLIENT_NO_AUTH = "airtime"

    def test_empty_string_org(self):
        empty = asa.Client("")
        self.assertTrue(empty.orgs == [])

    
    # incorrect client name should return empty string
    def test_fake_org(self):
        invalid_client = asa.Client(self.CLIENT_INVALID)
        self.assertTrue(invalid_client.orgs == [])

          
    def test_real_org(self):
        valid_client = asa.Client(self.CLIENT_VALID_LITTLE_DATA)
        # manually confirmed this org id
        # if CLIENT_VALID_LITTLE_DATA changes this must change
        self.assertTrue(valid_client.orgs == ["202690"])


    # test client with multiple orgs
    def test_multiple_orgs(self):
        client_multiple_orgs = asa.Client(self.CLIENT_MULTIPLE_ORGS)
        # manually confirmed these org ids
        # if CLIENT_MULTIPLE_ORGS changes this must change
        self.assertTrue(client_multiple_orgs.orgs == ["1429160", "1429260", "1429250"])

    def test_normal_campaigns_cols(self):
        valid_client = asa.Client(self.CLIENT_VALID_LITTLE_DATA)
        campaigns = valid_client.get_campaigns()
        returned_cols = list(campaigns.columns)
        expected_cols = [
        'adamId', 
        'budgetAmount.amount', 
        'budgetAmount.currency',
        'countriesOrRegions', 
        'deleted', 
        'displayStatus', 
        'endTime',
        'id', 
        'modificationTime', 
        'name', 
        'orgId', 
        'paymentModel', 
        'sapinLawResponse',
        'servingStateReasons', 
        'servingStatus', 
        'startTime', 
        'status'
        ]

        self.assertTrue(set(returned_cols).issuperset(set(expected_cols)))
  
    def test_unauthorized_campaigns_no_df(self):
        client_no_auth = asa.Client(self.CLIENT_NO_AUTH)
        campaigns = client_no_auth.get_campaigns(df=False)
        self.assertTrue(campaigns == [])

    def test_unauthorized_campaigns_df(self):
        client_no_auth = asa.Client(self.CLIENT_NO_AUTH)
        campaigns = client_no_auth.get_campaigns(df=True)
        self.assertTrue(pd.DataFrame().empty)

    def test_campaigns_for_speficic_org(self):
        client_multiple_orgs = asa.Client(self.CLIENT_MULTIPLE_ORGS)
        # manually checked this org ID
        # must change if CLIENT_MULTIPLE_ORGS changes
        org_id = "1429160"
        campaigns = client_multiple_orgs.get_campaigns(org_id=org_id)
        returned_org = str(list(campaigns['orgId'].unique())[0])
        self.assertTrue(org_id == str(returned_org))

    def test_valid_campaign_ids(self):
        valid_client = asa.Client(self.CLIENT_VALID_LITTLE_DATA)
        campaigns = valid_client.get_campaign_ids()
        self.assertIsInstance(campaigns, list)

    def test_invalid_campaign_ids(self):
        valid_client = asa.Client(self.CLIENT_INVALID)
        campaigns = valid_client.get_campaign_ids()
        self.assertTrue(campaigns == [])

    # adgroup tests
    """
    def test_correct_client_correct_adgroup(self):
        valid_client = asa.Client(self.CLIENT_VALID_LITTLE_DATA)
        # manually confirmed this campaign ID
        # if CLIENT_VALID_LITTLE_DATA changes this must change
        adgroups = valid_client.get_adgroups(11743281)
        returned_cols = list(adgroups.columns)
        expected_cols = [
        'automatedKeywordsOptIn',
        'campaignId',
        'cpaGoal',
        'defaultCpcBid.amount',
        'defaultCpcBid.currency',
        'deleted',
        'displayStatus',
        'endTime',
        'id',
        'modificationTime',
        'name',
        'orgId',
        'servingStateReasons',
        'servingStatus',
        'startTime',
        'status',
        'targetingDimensions.adminArea',
        'targetingDimensions.age',
        'targetingDimensions.appDownloaders.excluded',
        'targetingDimensions.appDownloaders.included',
        'targetingDimensions.country',
        'targetingDimensions.daypart',
        'targetingDimensions.deviceClass.included',
        'targetingDimensions.gender',
        'targetingDimensions.locality'
        ]

        self.assertTrue(set(expected_cols).issuperset(set(returned_cols)))
    """

    def test_correct_client_invalid_adgroup_df(self):
        valid_client = asa.Client(self.CLIENT_VALID_LITTLE_DATA)
        # invalid campaign ID
        adgroups = valid_client.get_adgroups(12345, df=True)
        self.assertTrue(adgroups.empty)

    # apple is so bad this actually return a 200 respose
    # thus this test is on hold
    #def test_correct_client_invalid_adgroup_no_df(self):
    #    accuweather = asa.Client("accuweather")
    #    # invalid campaign ID
    #    adgroups = accuweather.get_adgroups(12345, df=False)
    #    self.assertTrue(adgroups == [])

    def test_incorrect_client_incorrect_adgroup_df(self):
        invalid_client = asa.Client(self.CLIENT_INVALID)
        # invalid campaign ID
        adgroups = invalid_client.get_adgroups(12345, df=True)
        self.assertTrue(adgroups.empty)

    def test_incorrect_client_incorrect_adgroup_no_df(self):
        invalid_client = asa.Client(self.CLIENT_INVALID)
        # invalid campaign ID
        adgroups = invalid_client.get_adgroups(12345, df=False)
        self.assertTrue(adgroups == [])

    def test_get_adgroup_ids_invalid_client(self):
        invalid_client = asa.Client(self.CLIENT_INVALID)
        adgroups = invalid_client.get_adgroup_ids()
        self.assertTrue(adgroups == [])

    def test_get_adgroup_ids_invalid_client_org(self):
        invalid_client = asa.Client(self.CLIENT_INVALID)
        adgroups = invalid_client.get_adgroup_ids(org_id=123)
        self.assertTrue(adgroups == [])

    def test_get_adgroup_ids_invalid_client_status(self):
        invalid_client = asa.Client(self.CLIENT_INVALID)
        adgroups = invalid_client.get_adgroup_ids(status="PAUSED")
        self.assertTrue(adgroups == [])

    def test_get_adgroup_ids_valid_client(self):
        valid_client = asa.Client(self.CLIENT_VALID_LITTLE_DATA)
        adgroups = valid_client.get_adgroup_ids()
        self.assertIsInstance(adgroups, list)

    def test_get_adgroup_ids_valid_client_multiple_orgs(self):
        client_multiple_orgs = asa.Client(self.CLIENT_MULTIPLE_ORGS)
        adgroups = client_multiple_orgs.get_adgroup_ids()
        # ebay has multiple orgs
        # set of unique orgs returned
        unique_orgs = len({c[0] for c in adgroups})
        self.assertTrue(unique_orgs > 1)

    def test_get_adgroup_ids_valid_client_one_org(self):
        client_multiple_orgs = asa.Client(self.CLIENT_MULTIPLE_ORGS)
        adgroups = client_multiple_orgs.get_adgroup_ids(org_id=1429160)
        # ebay has multiple orgs
        # set of unique orgs returned
        unique_orgs = len({c[0] for c in adgroups})
        self.assertTrue(unique_orgs == 1)

    def test_get_keywords_invalid_client_no_df(self):
        invalid_client = asa.Client(self.CLIENT_INVALID)
        keywords = invalid_client.get_keywords(123, 123, df=False)
        self.assertTrue(keywords == [])

    def test_get_keywords_invalid_client_df(self):
        invalid_client = asa.Client(self.CLIENT_INVALID)
        keywords = invalid_client.get_keywords(123, 123, df=True)
        self.assertTrue(keywords.empty)

    def test_get_keywords_valid_client_df(self):
        valid_client = asa.Client(self.CLIENT_VALID_LITTLE_DATA)
        keywords = valid_client.get_keywords(282550033, 282583476, df=True)
        returned_cols = list(keywords.columns)
        expected_cols = [
            'adGroupId',
            'bidAmount.amount',
            'bidAmount.currency',
            'deleted',
            'id',
            'matchType',
            'modificationTime',
            'status',
            'text'
        ]
        self.assertListEqual(returned_cols, expected_cols)

    def test_get_keywords_valid_client_invalid_campaign_df(self):
        valid_client = asa.Client(self.CLIENT_VALID_LITTLE_DATA)
        keywords = valid_client.get_keywords(123, 108882501, df=True)
        self.assertTrue(keywords.empty)

    # all keywords
    def test_get_all_keywords_invalid_client_df(self):
        invalid_client = asa.Client(self.CLIENT_INVALID)
        keywords = invalid_client.get_all_keywords(df=True)
        self.assertTrue(keywords.empty)

    # 
    def test_get_all_keywords_invalid_client_no_df(self):
        invalid_client = asa.Client(self.CLIENT_INVALID)
        keywords = invalid_client.get_all_keywords(df=True)
        self.assertTrue(keywords.empty)

    # valid client
    def test_get_all_keywords_valid_client_df(self):
        valid_client = asa.Client(self.CLIENT_VALID_LITTLE_DATA)
        keywords = valid_client.get_all_keywords(df=True)
        returned_cols = list(keywords.columns)
        expected_cols = [
            'adGroupId',
            'bidAmount.amount',
            'bidAmount.currency',
            'deleted',
            'id',
            'matchType',
            'modificationTime',
            'status',
            'text'
        ]
        self.assertListEqual(returned_cols, expected_cols)

    def test_get_all_keywords_valid_client_no_df(self):
        valid_client = asa.Client(self.CLIENT_VALID_LITTLE_DATA)
        keywords = valid_client.get_all_keywords(df=False)
        bool_value = type(keywords) == list and len(keywords) >= 1
        self.assertTrue(bool_value)
        # self.assertTrue(keywords == [])

    def test_get_neg_keywords_invalid_client_no_df(self):
        invalid_client = asa.Client(self.CLIENT_INVALID)
        neg_keywords = invalid_client.get_negative_keywords(123, 123, df=False)
        self.assertTrue(neg_keywords == [])

    def test_get_neg_keywords_invalid_client_df(self):
        invalid_client = asa.Client(self.CLIENT_INVALID)
        neg_keywords = invalid_client.get_negative_keywords(123, 123, df=True)
        self.assertTrue(neg_keywords.empty)

    def test_get_neg_keywords_valid_client_df(self):
        valid_client = asa.Client(self.CLIENT_VALID_LITTLE_DATA)
        neg_keywords = valid_client.get_negative_keywords(282550033, 282583476, df=True)
        returned_cols = list(neg_keywords.columns)
        expected_cols = [
            'adGroupId',
            'bidAmount.amount',
            'bidAmount.currency',
            'deleted',
            'id',
            'matchType',
            'modificationTime',
            'status',
            'text'
        ]
        self.assertListEqual(returned_cols, expected_cols)

    def test_get_neg_keywords_valid_client_invalid_campaign_df(self):
        valid_client = asa.Client(self.CLIENT_VALID_LITTLE_DATA)
        neg_keywords = valid_client.get_negative_keywords(123, 108882501, df=True)
        self.assertTrue(neg_keywords.empty)

    # all keywords
    def test_get_all_neg_keywords_invalid_client_df(self):
        invalid_client = asa.Client(self.CLIENT_INVALID)
        neg_keywords = invalid_client.get_all_negative_keywords(df=True)
        self.assertTrue(neg_keywords.empty)

    # 
    def test_get_all_neg_keywords_invalid_client_no_df(self):
        invalid_client = asa.Client(self.CLIENT_INVALID)
        neg_keywords = invalid_client.get_all_negative_keywords(df=True)
        self.assertTrue(neg_keywords.empty)

    # valid client
    def test_get_all_neg_keywords_valid_client_df(self):
        valid_client = asa.Client(self.CLIENT_VALID_LITTLE_DATA)
        neg_keywords = valid_client.get_all_negative_keywords(df=True)
        returned_cols = list(neg_keywords.columns)
        expected_cols = [
            'adGroupId', 
            'campaignId', 
            'deleted', 
            'id', 
            'matchType',
            'modificationTime', 
            'status', 
            'text',
        ]
        self.assertListEqual(returned_cols, expected_cols)

    def test_get_all_keywords_valid_client_no_df(self):
        valid_client = asa.Client(self.CLIENT_VALID_LITTLE_DATA)
        neg_keywords = valid_client.get_all_negative_keywords(df=False)
        bool_value = type(neg_keywords) == list and len(neg_keywords) >= 1
        self.assertTrue(bool_value)
        # self.assertTrue(keywords == [])