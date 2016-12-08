#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Compare aiohttp and grequests
'''
import logging
import hashlib
import asyncio
import time
import aiohttp

from hdx.data.resource import Resource
from hdx.facades.simple import facade

logger = logging.getLogger(__name__)

NUMBER_OF_URLS_TO_PROCESS = 10

async def fetch(metadata, session):
    url, resource_id = metadata
    md5hash = hashlib.md5()
    try:
        with aiohttp.Timeout(300, loop=session.loop):
            async with session.get(url) as response:
                last_modified = response.headers.get('Last-Modified', None)
                if last_modified:
                    response.close()
                    return resource_id, url, 1, last_modified
                logger.info('Hashing %s' % url)
                async for chunk in response.content.iter_chunked(10240):
                    if chunk:
                        md5hash.update(chunk)
                return resource_id, url, 2, md5hash.hexdigest()
    except Exception as e:
        return resource_id, url, 0, str(e)


async def aiohttp_check_resources_for_last_modified(last_modified_check, limit, loop):
    tasks = list()

    conn = aiohttp.TCPConnector(conn_timeout=10, limit=limit)
    async with aiohttp.ClientSession(connector=conn, loop=loop) as session:
        for metadata in last_modified_check:
            task = fetch(metadata, session)
            tasks.append(task)
        return await asyncio.gather(*tasks)


def print_results(results):
    lastmodified_count = 0
    hash_count = 0
    failed_count = 0
    for resource_id, url, status, result in results:
        if status == 0:
            failed_count += 1
            logger.error(result)
        elif status == 1:
            lastmodified_count += 1
        elif status == 2:
            hash_count += 1
        else:
            raise ValueError('Invalid status returned!')
    str = 'Have Last-Modified: %d, Hashed: %d, ' % (lastmodified_count, hash_count)
    str += 'Number Failed: %d' % failed_count
    logger.info(str)


def run_aiohttp(last_modified_check, limit):
    start_time = time.time()
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(aiohttp_check_resources_for_last_modified(last_modified_check, limit, loop))
    results = loop.run_until_complete(future)
    logger.info('Limit: %d, Execution time: %s seconds' % (limit, time.time() - start_time))
    print_results(results)


def main(configuration):
    resources = Resource.search_in_hdx(configuration, 'name:')
    last_modified_check = list()
    for resource in resources:
        if not 'worldpop' in resource['name'].lower():
            continue
        resource_id = resource['id']
        url = resource['url']
        if 'data.humdata.org' in url or 'manage.hdx.rwlabs.org' in url or 'proxy.hxlstandard.org' in url or \
                'scraperwiki.com' in url or 'ourairports.com' in url:
            continue
        last_modified_check.append((url, resource_id))
    last_modified_check = sorted(last_modified_check)[:NUMBER_OF_URLS_TO_PROCESS]
    run_aiohttp(last_modified_check, 1)
    run_aiohttp(last_modified_check, 10)

if __name__ == '__main__':
    facade(main, hdx_site='prod', hdx_read_only=True)
