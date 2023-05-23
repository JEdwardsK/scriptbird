# For all accounts with 3 or more published email campaigns: get the number of
# identified persons, number of anonymous persons
# Outputs CSV

from auth.models import Account
from app.models.domain import Domain
from app.models.campaign import Campaign
from app.models.person_segment import PersonSegment
import csv


def agg_accounts_with_gte_3_published_email_campaigns():
    email_campaigns_cls = [
        'Campaign.PinpointEmailCampaign',
        'Campaign.PinpointEmailCampaign.SmartPinpointEmailCampaign',
        'Campaign.PinpointEmailCampaign.SmartPinpointEmailCampaign.AutoPinpointEmailCampaign',
    ]

    email_campaigns_agg = Campaign._get_collection().aggregate(
        [
            {
                "$match": {
                    "state": 'published',
                    "_cls": {
                        "$in": email_campaigns_cls
                    },
                    "account": {'$exists': True}
                }
            },
            {
                "$group": {
                    "_id": "$account",
                    "count": {"$sum": 1},
                }
            },
        ],
        allowDiskUse=True,
        cursor={},
    )

    accounts = []
    for campaign_count_by_account in email_campaigns_agg:
        if campaign_count_by_account.get('count') >= 3:
            accounts.append(campaign_count_by_account.get('_id'))

    return accounts


def all_persons_count(account_id):
    segment = get_helper_segment(account_id)
    return segment.count_persons()


def all_cookies_count(account_id):
    segment = get_helper_segment(account_id)
    return segment.aggregate_field_cardinality()


def all_emails_count(account_id):
    segment = get_helper_segment(account_id)
    return segment.aggregate_field_cardinality(field="email")


def get_helper_segment(account_id):
    return PersonSegment(
        _parent=account_id,
        predicates=[],
        lookback_mode="unbounded",
    )


def report():
    accounts = agg_accounts_with_gte_3_published_email_campaigns()
    with open("accounts_with_3_or_more_email_campaigns.csv", "w") as csvfile:
        writer = csv.writer(csvfile)
        headers = ["Account ID", "Account Name", "All Persons Count", "Identified Persons (has email)", "Anonymous Persons"]
        writer.writerow(headers)
        for account_id in accounts:
            print(account_id)
            account = Account.objects.get(pk=account_id)
            all_count = all_persons_count(account.id)
            emails_count = all_emails_count(account.id)
            anon_count = all_count - emails_count
            writer.writerow([account.id, account.name, all_count, emails_count, anon_count])
