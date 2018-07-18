from __future__ import print_function

import re
import traceback
import yaml
import time
import datetime
from datetime import timedelta
import requests
import ec2hosts
import sys
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.exceptions import ConnectionTimeout, NotFoundError
import elasticsearch
from os import environ

client = elasticsearch.Elasticsearch
requests.packages.urllib3.disable_warnings()  # turn off requests library warnings

# This is the entry point where Lambda will start execution.
def handler(event, context):
    # open the alias list file to roll through and tag indexes with the datemath alias
    with open('serverless_rollover.yaml') as config_file:
        config = yaml.load(config_file)
    print(config)

    datem = datetime.datetime.utcnow().strftime('%Y.%m.%d')  # Today's timestamp YYYY.mm.dd for aliasing
    date_plain = datetime.datetime.utcnow()  # todays UTC date for datemath later in alias curation
    print(date_plain)  # debugging in lambda

    # get credentials from ENV [This can be used in GitLab CICD + AWS Lambda]
    try:
        user_name = environ['ES_USER']
        user_pass = environ['ES_PASS']
    except KeyError:
        print("Missing ES_USER, ES_PASS")
        raise

    if "ES_ENDPOINTS" in environ:
        if environ["ES_ENDPOINTS"] == "auto":
            try:
                es_endpoints = ec2hosts.get_masters(public_dns=True)
                print("ES Endpoints: {0}".format(es_endpoints))
            except Exception as e:
                print("error getting es endpoints")
                print(repr(e))
                print(traceback.format_exc())
                sys.exit(1)
        else:
            es_endpoints = environ["ES_ENDPOINTS"].split(",")
    else:
        raise RuntimeError("Missing ES_ENDPOINTS variable")

    # to be used with multiple clusters. This is not important for our run for now
    # keeping for potential future use
    for cluster_config in config:
        cluster_name = cluster_config['name']

        # Create a connection to the cluster. We're using mangaged clusters in
        # Elastic Cloud for this example, so we can enable SSL security.
        es = Elasticsearch(es_endpoints, http_auth=(user_name, user_pass), port=9200, use_ssl=True,
                           verify_certs=False, connection_class=RequestsHttpConnection,retry_on_timeout=True,
                           max_retries=3)

        # START of the Aliasing API code section
        index_list = es.indices.get_alias(name="*_hot")
        # pull the rollover index name, remove the -000001 suffix
        # and then add an alias based on the prefix_index_datemath
        for index in index_list:
            for i in range(30):
                try:
                    regex = re.match('(.*)(?=\-\d+)', index)
                    index_name = regex.group(1)
                    # print(index_name)
                    # print("{}_{}".format(index_name, datem))
                    es.indices.put_alias(index=index, name="{}_{}".format(index_name, datem))
                    print(
                        'Index \'{}\' was aliased with \'{}_{}\' for searching purposes.'.format(index, index_name,
                                                                                                 datem))
                except ConnectionTimeout:
                    print("Connection Timeout. Retrying in 2 seconds")
                    time.sleep(2)
                    continue
                else:
                    break
            else:
                print("FAILED: Connection To Server Timed Out Too Many Times.")

        # START of the rollover API code section
        alias_list = es.indices.get_alias(name="*_hot")
        for index_keys, alias_keys in alias_list.items():
            names = alias_keys.items()
            for alias, values in names:
                for alias_names in values:
                    print('Checking "%s" alias on %s cluster for rollover.' % (alias_names, cluster_name))
                    # rolls over the index if it matches the following criteria
                    # it saves it as variable `roll` for if statements
                    roll = es.indices.rollover(alias=alias_names, wait_for_active_shards='2',
                                               body='{ "conditions": { "max_docs":  "135000000" }}')
                    # if it rolls over, the following steps are taken
                    if roll['acknowledged'] is True:
                        print('Alias \'%s\' was rolled over. Now running serverless-aliasing' % alias)
                        index_list = es.indices.get_alias(name=alias_names)
                        # pull the rollover index name, remove the -000001 suffix
                        # and then add an alias based on the prefix_index_datemath
                        for index in index_list:
                            for i in range(30):
                                try:
                                    regex = re.match('(.*)(?=\-\d+)', index)
                                    index_name = regex.group(1)
                                    # print(index_name)
                                    # print("{}_{}".format(index_name, datem))
                                    es.indices.put_alias(index=index, name="{}_{}".format(index_name, datem))
                                    print('\'{}\' was aliased with \'{}_{}\''
                                          ' for searching purposes.'.format(index, index_name, datem))
                                except ConnectionTimeout:
                                    print("Connection Timeout. Retrying in 2 seconds")
                                    time.sleep(2)
                                    continue
                                else:
                                    break
                    # if it does not roll over, it just moves on
                    else:
                        print('Alias \'%s\' was not rolled over.' % alias_names)

        # START of delete old aliases code section
        # lots of for loops b/c ES JSON is ugly. Sorry.
        all_alias_list = es.indices.get_alias(index="*")
        for index, aliases in all_alias_list.items():
            names = aliases.items()
            for alias, values in names:
                for alias_names in values:
                    # we will target only specific aliases in order to not delete the wrong hidden indices
                    # this will also be sure that we are selecting  indices based on
                    # index names of syslog-ng_index-000001, other_index-000001
                    if ("syslog-ng_" in index or "other_" in index) \
                            and "aliases" in aliases \
                            and bool(aliases["aliases"]) is True:
                        for index_name in cluster_config['indices']:
                            alias_by_index = re.search(index_name['name'], alias_names)
                            if alias_by_index and "hot" not in alias_names:
                                # search the alias name for the date
                                regex_search = re.search('([12]\d{3}\.(0[1-9]|1[0-2])\.(0[1-9]|[12]\d|3[01]))', alias_names)
                                # if the search doesn't return "None", as in it returns an actual result, then continue
                                # if regex_search:
                                if regex_search:
                                    # print('checking %s for age' % alias_names)
                                    # if the date stamp is older than retention period, delete the alias
                                    regex_match = regex_search.group(1)
                                    regex_date = datetime.datetime.strptime(regex_match, '%Y.%m.%d')
                                    retention = index_name.get('days', 365)
                                    duration = date_plain - timedelta(days=retention)
                                    if regex_date < duration:
                                        try:
                                            print('Deleting %s alias because %s has a %s retention period'
                                                  % (alias_names, index_name['name'], retention))
                                            es.indices.delete_alias(index='_all', name=alias_names)
                                        except NotFoundError:
                                            print("Deleted multiple %s aliases. Moving to next alias" % alias_names)
                                            continue
                                        except ConnectionTimeout:
                                            print("Connection Timeout. Retrying in 2 seconds")
                                            time.sleep(2)
                                            continue
                                        else:
                                            break
        # This actually deletes any index without any aliases
        all_index_list = es.indices.get(index="*")
        for index, aliases in all_index_list.items():
            if "syslog-ng" in index or "other" in index:
                if "aliases" in aliases and bool(aliases["aliases"]) is False:
                    print('Deleting %s because it has no aliases tied to it' % index)
                    es.indices.delete(index=index)
