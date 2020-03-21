import re
from datetime import datetime

import click
import singer
import requests
from bs4 import BeautifulSoup

STREAM_NAME = 'cdc_covid19_top_stats_daily'

STREAM_KEY_PROPERTIES = [
    'date'
]

SCHEMA = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        '__datetime_captured': {
            'type': [
                'string'
            ],
            'format': 'date-time'
        },
        'date': {
            'type': [
                'string'
            ],
            'format': 'date-time'
        },
        'total_cases': {
            'type': [
                'null',
                'integer'
            ]
        },
        'total_deaths': {
            'type': [
                'null',
                'integer'
            ]
        },
        'travel_related': {
            'type': [
                'null',
                'integer'
            ]
        },
        'close_contact': {
            'type': [
                'null',
                'integer'
            ]
        },
        'under_investigation': {
            'type': [
                'null',
                'integer'
            ]
        }
    }
}

@click.group()
def main():
    pass

def remove_comma(str):
    return str.replace(',', '')

def match_total_li(li, regex):
    return remove_comma(re.match(regex, li.text).groups()[0])

@main.command('sync')
def sync():
    response = requests.get('https://www.cdc.gov/coronavirus/2019-ncov/cases-updates/cases-in-us.html')
    response.raise_for_status()

    doc = BeautifulSoup(response.text, 'lxml')

    # updated date 
    updated_el = doc.select('body > div.container.d-flex.flex-wrap.body-wrapper.bg-white > main > div:nth-child(3) > div > div:nth-child(3) > div:nth-child(1) > div > p:nth-child(2) > span')
    raw_date = re.match(r'Updated (.+)', updated_el[0].text).groups()[0]
    updated_date = datetime.strptime(raw_date, '%B %d, %Y')

    # top total stats
    lis = doc.select('body > div.container.d-flex.flex-wrap.body-wrapper.bg-white > main > div:nth-child(3) > div > div:nth-child(3) > div:nth-child(2) ul > li')
    total_cases = int(match_total_li(lis[0], r'Total cases: ([0-9,]+)'))
    total_deaths = int(match_total_li(lis[1], r'Total deaths: ([0-9,]+)'))

    # by source
    table_row_els = doc.select('body > div.container.d-flex.flex-wrap.body-wrapper.bg-white > main > div:nth-child(3) > div > div:nth-child(3) > div:nth-child(2) > div > div > div > div.card-body.bg-white > table > tbody > tr')
    travel_related = int(remove_comma(table_row_els[0].select('td')[1].text))
    close_contact = int(remove_comma(table_row_els[1].select('td')[1].text))
    under_investigation = int(remove_comma(table_row_els[2].select('td')[1].text))
    source_total_cases = int(remove_comma(table_row_els[3].select('td')[1].text))

    assert travel_related + close_contact + under_investigation == source_total_cases
    assert source_total_cases == total_cases

    record = {
        '__datetime_extracted': datetime.utcnow().isoformat(),
        'date': updated_date.isoformat(),
        'total_cases': total_cases,
        'total_deaths': total_deaths,
        'travel_related': travel_related,
        'close_contact': close_contact,
        'under_investigation': under_investigation
    }

    singer.write_schema(STREAM_NAME, SCHEMA, STREAM_KEY_PROPERTIES)
    singer.write_record(STREAM_NAME, record)

@main.command('test')
def test():
    pass

if __name__ == '__main__':
    main()
