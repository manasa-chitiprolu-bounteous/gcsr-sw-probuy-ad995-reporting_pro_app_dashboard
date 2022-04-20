from google.cloud import bigquery
import json
from utils import view_to_table

def main():
    project = 'sw-probuy-ad995'
    dataset_id = 'reporting'
    tables = ['pro_app_item_level','pro_app_user_behavior']

    try:
        for table in tables:
            view_to_table(project, dataset_id, table)
        return json.dumps({'success': True}), 200, {'ContentType': 'application/json'}

    except Exception as e:
        print('There was an error in deleting/writing data to BQ : ' + str(e))
        return json.dumps({'success': False}), 400, {'ContentType': 'application/json'}
