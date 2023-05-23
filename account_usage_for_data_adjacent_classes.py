# For accounts within their first active year: find number of auth'd domains, number of active
# conversion pixels, number of segments, number of custom fields, number of active projects.
# Outputs CSV
from auth.models import Account
from app.models.domain import Domain
from app.models.person_segment import PersonSegment
from app.models.account_parameter import AccountParameter
from app.models.event import Event
from datetime import datetime, timedelta, timezone
import csv


def accounts_in_their_first_year():
    accounts = Account.objects()
    account_list = []
    for account in accounts:
        one_year_ago = datetime.now(timezone.utc) - timedelta(days=365)
        if account.id.generation_time > one_year_ago:
            account_list.append(account)

    return account_list


def get_auth_domain_count(account_id):
    return Domain.objects(
        is_archived__ne=True, account=account_id,
    ).count()


def get_conversion_pixel_count(account_id):
    return PersonSegment.objects(
        is_archived__ne=True, _parent=account_id, is_conversion_segment=True
    ).count()


def get_segment_count(account_id):
    return PersonSegment.objects(
        is_archived__ne=True, _parent=account_id, is_conversion_segment=False
    ).count()


def get_custom_fields_count(account_id):
    return AccountParameter.objects(
        _parent=account_id, is_archived__ne=True, u_key__exists=True
    ).count()


def get_projects_count(account_id):
    return Event.objects(is_archived__ne=True, _owner=account_id).count()


def report():
    accounts = accounts_in_their_first_year()
    with open('accounts_in_their_first_year.csv', 'w') as csvfile:
        writer = csv.writer(csvfile)
        headers = ['Account ID', 'Account Name', 'Authorized Domains', 'Conversion Pixels', 'Segments', 'Custom Fields', 'Projects']
        writer.writerow(headers)
        for account in accounts:
            print(account.id)
            writer.writerow([
                account.id,
                account.name,
                get_auth_domain_count(account.id),
                get_conversion_pixel_count(account.id),
                get_segment_count(account.id),
                get_custom_fields_count(account.id),
                get_projects_count(account.id),
            ])