# -*- coding: utf-8 -*-
# (c) 2017-2019 Andreas Motl <andreas@hiveeyes.org>
import os
import sys
import json
import requests
from copy import copy
from pprint import pprint
from bs4 import BeautifulSoup
from collections import OrderedDict
"""
Scrape data from http://hivetool.net/.

Examples:
- http://hivetool.net/Anastasia
- http://hivetool.net/BEE-SIDE01

Synopsis:
- Fetch metadata
- Upload metadata
- Fetch data
- Upload data::

    cat tmp/77.json | http POST https://swarm.hiveeyes.org/api/hiveeyes/testdrive-hivetool/test1/77/data Content-Type:text/csv
    https://swarm.hiveeyes.org/grafana/dashboard/db/hiveeyes-testdrive-hivetool-automatic

"""

class HiveToolMetadata(object):

    STATUS_URL = 'http://hivetool.net/node/69'

    def get_list(self):
        response = requests.get(self.STATUS_URL)
        soup = BeautifulSoup(response.content, "html.parser")
        #print soup

        entries = []
        for link in soup.find_all('a'):
            href = link.get('href')
            #print href
            if href and 'hive_stats.pl' in href:
                #print href
                row = link.find_parent('tr')
                columns = row.find_all('td')
                #print columns

                name = columns[0].find('a').contents[0].strip()
                link = columns[0].find('a').get('href').strip()
                location = columns[2].contents[0].strip(',\t ')
                last_seen = columns[3].contents[0].strip()

                entry = {
                    'name': name,
                    'link': link,
                    'location': location,
                    'last_seen': last_seen,
                    'hive_id': link.replace('http://hivetool.org/db/hive_stats.pl?hive_id=', ''),
                }
                entries.append(entry)

        return entries

    def get_info(self, link):
        response = requests.get(link)
        soup = BeautifulSoup(response.content, "html.parser")
        name = soup.find('title').contents[0].replace('HiveTool: ', '').replace(' ', '')

        link = 'http://hivetool.net/' + name
        response = requests.get(link)

        #rdfa_parser = rdfadict.RdfaParser()
        #triples = rdfa_parser.parse_string(response.content, link)
        #print 'triples:', triples

        soup = BeautifulSoup(response.content, "html.parser")
        #print soup
        #print

        extraction_rules = [

            # <meta about="/Anastasia" content="Anastasia" property="dc:title"/>
            {
                'name': 'title',
                'attrs': {'property': 'dc:title'},
                'extract': {'attribute': 'content'},
            },

            # <div class="field field-name-field-type field-type-list-text field-label-above"><div class="field-label">Type: </div><div class="field-items"><div class="field-item even">Langstroth 10 frame</div></div></div>
            {
                'name': 'beehive_type',
                'attrs': {'class': 'field-name-field-type'},
                'find': {'class': 'field-item'},
            },

            # <span content="2016-05-17T10:51:44-04:00" datatype="xsd:dateTime" property="dc:date dc:created" rel="sioc:has_creator">
            #   [...]
            # </span>
            {
                'name': 'created',
                'attrs': {'class': 'meta submitted'},
                'find': {'property': 'dc:date dc:created'},
                'extract': {'attribute': 'content'},
            },

            # Submitted by <span about="/user/183" class="username" property="foaf:name" typeof="sioc:UserAccount" xml:lang="">Carl</span>
            {
                'name': 'username',
                'attrs': {'class': 'meta submitted'},
                'find': {'property': 'foaf:name'},
            },

            # <div class="location vcard">
            # <div class="adr">
            # <div class="country-name">United States</div>
            # <span class="geo"><abbr class="latitude" title="35.584316">35° 35' 3.5376" N</abbr>, <abbr class="longitude" title="-82.627316">82° 37' 38.3376" W</abbr></span>
            # </div>
            # <div class="map-link">
            # <div class="location map-link">See map: <a href="http://maps.google.com?q=35.584316+-82.627316+%28%2C+%2C+%2C+%2C+us%29">Google Maps</a></div> </div>
            # </div>
            # </div>
            #{
            #    'name': 'location',
            #    'attrs': {'class': 'location vcard'},
            #},
            {
                'container': 'location',
                'name': 'country',
                'attrs': {'class': 'location vcard'},
                'find': {'class': 'country-name'},
            },
            {
                'container': 'location',
                'name': 'region',
                'attrs': {'class': 'location vcard'},
                'find': {'class': 'region'},
            },
            {
                'container': 'location',
                'name': 'locality',
                'attrs': {'class': 'location vcard'},
                'find': {'class': 'locality'},
            },
            {
                'container': 'location',
                'name': 'name',
                'attrs': {'class': 'location vcard'},
                'find': {'class': 'fn'},
            },
            {
                'container': 'location',
                'name': 'map_link',
                'attrs': {'class': 'location map-link'},
                'find': {'element': 'a'},
                'extract': {'attribute': 'href'},
            },

            # <abbr class="latitude" title="42.882898">42° 52' 58.4328" N</abbr>
            {
                'container': 'location',
                'name': 'latitude',
                'attrs': {'class': 'latitude'},
                'extract': {'attribute': 'title'},
            },
            # <abbr class="longitude" title="11.271310">11° 16' 16.716" E</abbr>
            {
                'container': 'location',
                'name': 'longitude',
                'attrs': {'class': 'longitude'},
                'extract': {'attribute': 'title'},
            },

            # <div class="field field-name-field-elevation field-type-number-integer field-label-above"><div class="field-label">Elevation: </div><div class="field-items"><div class="field-item even">2 115feet</div></div></div>
            {
                'container': 'location',
                'name': 'elevation',
                'attrs': {'class': 'field-name-field-elevation'},
                'find': {'class': 'field-item'},
            },

            # <div class="field field-name-field-orietation field-type-number-integer field-label-above"><div class="field-label">Orientation: </div><div class="field-items"><div class="field-item even">180degrees</div></div></div>
            {
                'container': 'location',
                'name': 'orientation',
                'attrs': {'class': 'field-name-field-orietation'},
                'find': {'class': 'field-item'},
            },

            # <div class="field field-name-field-nasa-designator field-type-text field-label-above"><div class="field-label">NASA designator:&nbsp;</div><div class="field-items"><div class="field-item even">TX001</div></div></div>
            {
                'name': 'nasa_designator',
                'attrs': {'class': 'field-name-field-nasa-designator'},
                'find': {'class': 'field-item'},
            },

            # <div class="field field-name-body field-type-text-with-summary field-label-above">
            # <div class="field-label">Description: </div>
            # <div class="field-items">
            # <div class="field-item even" property="content:encoded"><p>Hive type is "Dadant" 10 frames (typical in Italy)<br/>
            # The family has been created by artificial swarming in April 2016.<br/>
            # In July 2016 the family was on 10 frames.</p>
            # </div>
            # </div>
            # </div>
            # </div>
            {
                'name': 'description',
                'attrs': {'class': 'field-name-body'},
                'find': {'class': 'field-item'},
            },

            # <div class="field field-name-field-image field-type-image field-label-above"><div class="field-label">Image: </div><div class="field-items"><div class="field-item even"><img alt="" height="406" src="http://hivetool.net/sites/default/files/images/20160911_184244_p_0.jpg" typeof="foaf:Image" width="452"/></div></div></div>
            {
                'name': 'image',
                'attrs': {'typeof': 'foaf:Image'},
                'extract': {'attribute': 'src'},
            },

        ]

        metadata = OrderedDict(name=name, link=link)
        extract_from_html(soup, extraction_rules, metadata)

        #if 'map_link' in metadata:
        #    print 'map_link:', metadata['map_link']
        #    print 'result:', result
        #    metadata['map_link'] = result.find('a')['href']


        comment_extraction_rules = [

            # <span rel="sioc:has_creator"><span about="/user/221" class="username" property="foaf:name" typeof="sioc:UserAccount" xml:lang="">Enrico</span></span>
            {
                'name': 'author',
                'attrs': {'property': 'foaf:name'},
            },

            # <span content="2016-09-26T19:24:18-04:00" datatype="xsd:dateTime" property="dc:date dc:created">Mon, 09/26/2016 - 19:24</span>
            {
                'name': 'created',
                'attrs': {'property': 'dc:date dc:created'},
                'extract': {'attribute': 'content'},
            },

            # <h3 datatype="" property="dc:title"><a class="permalink" href="/comment/126#comment-126" rel="bookmark">Paul, I am really excited</a></h3>
            {
                'name': 'title',
                'attrs': {'property': 'dc:title'},
                'find': {'element': 'a'},
            },

            # <div class="field field-name-comment-body field-type-text-long field-label-hidden"><div class="field-items"><div class="field-item even" property="content:encoded"><p>Paul, I am really excited seeing quantitative data coming from my remote hive.<br/>
            # [...]
            {
                'name': 'body',
                'attrs': {'class': 'field-name-comment-body'},
                'find': {'class': 'field-item'},
            },

        ]

        metadata['comments'] = []
        comment_container_node = soup.find(id='comments')
        if comment_container_node:
            comment_nodes = comment_container_node.find_all(typeof='sioc:Post sioct:Comment')
            comments = []
            for comment_node in comment_nodes:
                #print '=' * 42
                #print comment_node
                comment = OrderedDict()
                extract_from_html(comment_node, comment_extraction_rules, comment)
                #print '-' * 42
                #print comment
                #print json.dumps(comment, indent=4)
                comments.append(comment)

            comments.reverse()
            metadata['comments'] = comments

        #print
        #pprint(dict(metadata))

        return metadata


class HiveToolData(object):

    # 2017
    # http://hivetool.org/db/hive_graph5m.pl?hive_id=113&units=Metric&begin=2017-07-28%2023:59:59&end=2017-08-04%2023:59:59&weight_filter=Raw&nasa_weight_dwdt=60&midnight=0&download=Download&download_file_format=csv
    # http://hivetool.org/db/hive_graph5m.pl?hive_id=113&units=Metric&begin=2017-07-28%2023:59:59&weight_filter=Raw&nasa_weight_dwdt=60&midnight=0&download=Download&download_file_format=csv
    # http://hivetool.org/db/hive_graph5m.pl?hive_id={hive_id}&units=Metric&begin=2010-01-01%2000:00:00&weight_filter=Raw&nasa_weight_dwdt=60&midnight=0&download=Download&download_file_format=csv

    # 2019
    # view-source:http://hivetool.net/db/hive_graph706.pl?chart=Temperature&start_time=2019-08-04+23%3A59%3A59&end_time=2019-08-11+23%3A59%3A59&hive_id=77&number_of_days=7&last_max_dwdt_lbs_per_hour=30&weight_filter=Raw&max_dwdt_lbs_per_hour=&days=&begin=&end=&units=Metric&undefined=Skip&download_data=Download&download_file_format=csv
    DATA_URL_TEMPLATE = 'http://hivetool.net/db/hive_graph706.pl?hive_id={hive_id}&start_time=2019-01-01&end_time=&number_of_days=30&last_max_dwdt_lbs_per_hour=60&weight_filter=Raw&max_dwdt_lbs_per_hour=&days=&begin=&end=&units=Metric&undefined=Skip&download_file_format=csv'

    def __init__(self, hive_id):
        self.hive_id = hive_id

    def fetch_csv(self):
        url = self.DATA_URL_TEMPLATE.format(hive_id=self.hive_id)
        response = requests.get(url)

        if response.status_code != 200:
            print(response.text)
            raise ValueError('Fetching data from URL {} failed'.format(url))

        soup = BeautifulSoup(response.content, "html.parser")
        raw = soup.find('body').get_text()
        #print(raw)

        buffer = []
        for line in raw.split('\n'):
            line = line.strip()
            if not line: continue
            if line.startswith('Date'):
                line = '## ' + line
            else:
                line = line.replace(' ', 'T', 1)
                line = line.replace(' ', '')
            buffer.append(line)

        return '\n'.join(buffer)

def extract_from_html(soup, rules, data):

    for rule in rules:
        name = rule['name']
        result = soup.find(attrs=rule['attrs'])
        #print name, rule['attrs'], result

        if not result: continue

        value = None
        if 'find' in rule:
            if 'element' in rule['find']:
                element = rule['find']['element']
                del rule['find']['element']
                result = result.find(element, **rule['find'])
            else:
                result = result.find(**rule['find'])

        if result:

            if 'extract' in rule:
                extraction_rule = rule['extract']
                if 'attribute' in extraction_rule:
                    attribute = extraction_rule['attribute']
                    value = result[attribute]

            else:
                value = result.get_text()

        #print name, value
        #print '-' * 42

        if 'container' in rule:
            container = rule['container']
            if container not in data:
                data[container] = OrderedDict()
            data[container][name] = value
        else:
            data[name] = value


def single_info(hive_id=None):
    url = u'http://hivetool.net/db/hive_stats.pl?hive_id={hive_id}'.format(hive_id=hive_id)
    info = HiveToolMetadata().get_info(url)
    print(json.dumps(info, indent=4))

def multi_info():

    metadata = HiveToolMetadata()

    beehives = metadata.get_list()
    pprint(beehives)

    for beehive in beehives:
        print('=' * 42)
        print(beehive['name'])
        print(beehive['link'])
        print('-' * 42)
        info = metadata.get_info(beehive['link'])
        print(json.dumps(info, indent=4))
        print()

def multi_fetch(overwrite=False):

    metadata = HiveToolMetadata()
    basedir = './var/spool/meta'

    beehives = metadata.get_list()
    indexfile = basedir + '/index.json'
    index = {
        'source': metadata.STATUS_URL,
        'list': copy(beehives),
    }
    json.dump(index, file(indexfile, 'w'), indent=4)
    print(json.dumps(index, indent=4))

    for beehive in beehives:

        print(beehive)

        hive_id = int(beehive['link'].replace('http://hivetool.org/db/hive_stats.pl?hive_id=', ''))
        filename = basedir + '/{hive_id:0>5}.json'.format(hive_id=hive_id)
        if not overwrite and os.path.exists(filename):
            print('Skipping hive name={name}, id={hive_id}. File {file} already exists.'.format(name=beehive['name'], hive_id=hive_id, file=filename))
            continue

        print('=' * 42)
        print(beehive['name'])
        print(beehive['link'])
        print(filename)
        print('-' * 42)

        info = metadata.get_info(beehive['link'])
        info['baseinfo'] = copy(beehive)
        json.dump(info, file(filename, 'w'), indent=4)

        print(json.dumps(info, indent=4))
        print()


def single_data(hive_id):
    htdata = HiveToolData(hive_id=hive_id)
    data = htdata.fetch_csv()
    print(data)


def main():

    action = sys.argv[1]
    hive_id = int(sys.argv[2])

    if action == 'info':
        single_info(hive_id)

    elif action == 'data':
        single_data(hive_id)

    # TODO
    #multi_info()
    #multi_fetch(overwrite=False)
    #multi_fetch(overwrite=True)


if __name__ == '__main__':
    main()
