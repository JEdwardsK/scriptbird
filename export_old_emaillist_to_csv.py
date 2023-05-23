from app import models
from auth.models import Account
from datetime import datetime
from dateutil.relativedelta import relativedelta
from app.integrations import tradedesk
import csv


today = datetime.today()
first_of_last_month = today.replace(day=1) - relativedelta(months=1)
first_of_current_month = today.replace(day=1)

date_end = first_of_last_month
# date_end = datetime(2023, 3, 1) # last time we actually ran this, if needed

print(f'Getting cpns from {date_end} to {first_of_current_month}')
older_cpns = models.EmailListCampaign.objects(
    date_end__gte=date_end,
    date_end__lt=first_of_current_month,
    state__ne='draft'
    ).all()
older_fpids = []
trgtble_fpid_map = {}
count = 0

for cpn in older_cpns:
    targetings = cpn.get_targetings()
    for targeting in targetings:
        try:
            targetable = targeting.get_target_data()
        except ValueError:
            continue
        i_targetable = tradedesk.get_integration(targetable, create=False)
        if not i_targetable:
            continue
        older_fpids.append(i_targetable.email_list_fp_id)
        account = Account.objects.get(pk=cpn.account)
        trgtble_fpid_map.update({i_targetable.email_list_fp_id: {
            'targetable_id': str(targetable.id),
            'targetable_name': f"{account.ttd.id}-{targetable.id}",
            'targetable_list_file': targetable.list_file,
            'account_name': account.name,
            'adv': i_targetable.ttd_adv.id,
        }})
    count += 1
    print(f'{count}. {cpn.id}, {targetable.id}, {cpn.date_start} - {cpn.date_end}')

fpids_to_remove = list(set(older_fpids))

trgtble_fpid_map = sorted(trgtble_fpid_map.items(), key=lambda tar: tar[1].get('account_name'))

with open('email_list_export.csv', 'w') as csvfile:
    writer = csv.writer(csvfile)
    headers = ['Account Name', 'Advertiser ID', 'List ID', 'List Name',]
    writer.writerow(headers)
    for tar in trgtble_fpid_map:
        if tar[0] not in fpids_to_remove:
            continue
        writer.writerow([tar[1].get('account_name'), tar[1].get('adv'), tar[1].get('targetable_id'), tar[1].get('targetable_name'),])

