import asyncio
import os
import re
from collections import Counter
from typing import Any

import aiohttp
import requests

HEADERS = {'Authorization': 'token ' + os.getenv('GITHUB_ACCESS_TOKEN')}
PARAMS = {'per_page': 100}
URL = 'https://api.github.com'


def get_repositories_count_from_organization(
        organization: str) -> int:
    request = requests.get(url=f'{URL}/orgs/{organization}',
                           headers=HEADERS)
    return request.json()['public_repos']


async def get_all_repositories_from_page(
        session: aiohttp.ClientSession,
        organization: str,
        page_number: int) -> list[str]:
    url = f'{URL}/orgs/{organization}/repos'
    params = PARAMS | {'page': page_number}
    async with session.get(url=url, params=params) as response:
        repositories = await response.json()
        return list(map(lambda x: x['name'], repositories))


async def get_all_commits_from_repository(
        session: aiohttp.ClientSession,
        organization: str,
        repository: str) -> list[dict[str, Any]]:
    commits, pages_count = await get_first_page_with_commits_and_pages_count(
        session, organization, repository)
    tasks = [get_all_commits_from_page(session, organization, repository, pn)
             for pn in range(2, pages_count + 1)]
    pages_with_commits = await asyncio.gather(*tasks)
    return commits + [commit for page in pages_with_commits for commit in page]


async def get_first_page_with_commits_and_pages_count(
        session: aiohttp.ClientSession,
        organization: str,
        repository: str) -> tuple[list[dict[str, Any]], int]:
    url = f'{URL}/repos/{organization}/{repository}/commits'
    async with session.get(url=url, params=PARAMS) as response:
        result = await response.json()
        if 'message' in result:
            return [], 0
        try:
            links = response.headers['link']
            number_of_pages_with_commits \
                = re.match('<.*page=(.*)>; rel="last"', links).group(1)
        except KeyError:
            number_of_pages_with_commits = 1
        except AttributeError:
            print('headers["link"] does not exist. wtf?')
            exit()
        commits = [commit['commit'] for commit in result]
        return commits, int(number_of_pages_with_commits)


async def get_all_commits_from_page(
        session: aiohttp.ClientSession,
        organization: str,
        repository: str,
        page_number: int) -> list[dict[str, Any]]:
    url = f'{URL}/repos/{organization}/{repository}/commits'
    params = PARAMS | {'page': page_number}
    async with session.get(url=url, params=params) as response:
        return [commit['commit'] for commit in await response.json()]


def count_statistics(
        commits: list[dict]) -> Counter:
    counter = Counter()
    for commit in commits:
        if 'Merge pull request' not in commit['message']:
            counter[commit['author']['email']] += 1
    return counter


def print_statistics(
        counter: Counter,
        count: int = 100):
    items = sorted(counter.items(), key=lambda x: x[1], reverse=True)
    for i, item in enumerate(items):
        if i < count:
            print(str(i + 1) + ')', item[0], '-', item[1])


async def main():
    organization = 'kontur-edu'
    repositories_count = get_repositories_count_from_organization(
        organization)
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        tasks = []
        for page_number in range(1, (repositories_count + 100 - 1) // 100 + 1):
            tasks.append(get_all_repositories_from_page(
                session,
                organization,
                page_number=page_number))
        repositories = []
        for result in asyncio.as_completed(tasks):
            repositories += await result
        tasks.clear()
        for repository in repositories:
            tasks.append(get_all_commits_from_repository(
                session,
                organization,
                repository))
        counter = Counter()
        for result in asyncio.as_completed(tasks):
            counter += count_statistics(await result)
    print_statistics(counter)

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
