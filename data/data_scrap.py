import requests
from bs4 import BeautifulSoup as bs
import asyncio
import aiohttp
import pickle


def get_urls():
    url = 'https://www.presidency.ucsb.edu/documents/' +\
        'presidential-documents-archive-guidebook/annual-messages-congress-the-state-the-union'
    page = requests.get(url)
    soup = bs(page.content, features="html.parser")
    a_objects = soup.find_all('a')
    urls = []
    years = []
    for a_object in a_objects:
        try:
            years.append(int(a_object.get_text()[:4]))
            if 'https' in a_object['href']:
                urls.append(a_object['href'])
        except:
            # +6 Nixon's speeches in 1973
            if 'State of the Union Message to the Congress' in a_object.get_text():
                urls.append(a_object['href'])
            pass
    return urls, years

async def get_speech_date(response):
    html = await response.text()
    soup = bs(html, features="html.parser")
    date = soup.find('span', class_='date-display-single').get_text()
    return date

async def get_speech_text(response):
    html = await response.text()
    soup = bs(html, features="html.parser")
    text_div = soup.find('div', class_='field-docs-content')
    texts = [i.get_text() for i in text_div.findChildren('p')]
    text = ' '.join(texts)
    return text

async def get_speech(session, url):
    async with session.get(url, ssl=False) as response:
        date = await get_speech_date(response)
        speech = await get_speech_text(response)
        return (speech, date)

def get_tasks(session, urls):
    tasks = []
    for url in urls:
        tasks.append(asyncio.ensure_future(get_speech(session, url)))
    return tasks

async def get_speeches(urls):
    async with aiohttp.ClientSession() as session:
        tasks = get_tasks(session, urls)
        speeches_dates = await asyncio.gather(*tasks)
    return speeches_dates

def async_run(urls):
    res = asyncio.run(get_speeches(urls))
    return res

if __name__ == '__main__':
    urls, years = get_urls()
    speeches_dates = async_run(urls)
    pickle.dump(speeches_dates, open('data/speeches_dates.pkl', 'wb'))
    # pickle.dump(urls, open('data/urls.pkl', 'wb'))
    # pickle.dump(years, open('years.pkl', 'wb'))