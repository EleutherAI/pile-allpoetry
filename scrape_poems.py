from bs4 import BeautifulSoup as bs
import requests
import re
import traceback
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
from lm_dataformat import Archive, Reader
import os
import argparse


# format :: https://allpoetry.com/poem/[number]

def split_into_chunks(l, n):
    n = max(1, n)
    return [l[i:i + n] for i in range(0, len(l), n)]


def value_to_float(x):
    if type(x) == float or type(x) == int:
        return x
    elif type(x) == str:
        x = x.strip().upper()
        if 'K' in x:
            if len(x) > 1:
                return float(x.replace('K', '')) * 1000
            return 1000.0
        elif 'M' in x:
            if len(x) > 1:
                return float(x.replace('M', '')) * 1000000
            return 1000000.0
        else:
            try:
                return float(x)
            except:
                return 0.0
    return 0.0


def parse_info(info):
    info = info.replace("views", "").replace("+list", "")
    info = " ".join(info.split())
    return (value_to_float(item) for item in info.split(" "))


def filter_triple_newline(text):
    return re.sub("\n\n+", "\n\n", text)


def scrape_poem(poem_id):
    """
    scrape a single poem from allpoetry.com/poem/[poem_id]

    :param poem_id: poem uid integer
    :return: {id: poem_id, views: n_views, likes: n_likes, comments: n_comments, text: poem_text}
    """
    response = requests.get("https://allpoetry.com/poem/{}".format(poem_id))
    if response.status_code != 200:
        raise ConnectionError("Response code != 200")
    soup = bs(response.content, "html.parser")
    info = soup.find("div", {"class": re.compile('.*item-info.*')}).text
    views, comments = parse_info(info)
    likes = value_to_float(
        soup.find("a", {"class": re.compile('^btn.*'), "alt": "Liked: "}).find("span", {"class": "num"}).text)
    poem = soup.find("div", {"class": "items_group main_poem"})
    title = poem.find("h1", {"class": re.compile('.*title.*')}).text
    body = poem.find("div", {"class": re.compile('^orig_.*')}).text
    return {"id": poem_id, "views": views, "likes": likes, "comments": comments,
            "text": filter_triple_newline(title + "\n\n" + body)}


def scrape_poem_mp(i):
    """
    wrapper fn for scrape_poem
    """
    try:
        try:
            poem = scrape_poem(i)
            return poem
        except (AttributeError, ConnectionError):
            pass
    except:
        traceback.print_exc()


def main(total_poems, chunk_size, pool, start_poem=1, commit_every=50, verbose=False):
    """
    scrape total_poems poems from allpoetry.com starting at poem_id = start_poem,
    and save them to a jsonl.zst object

    :param total_poems: int, total poems to scrape
    :param chunk_size: number of poems per chunk
    :param pool: multiprocessing pool,
    :param start_poem: poem id to start from
    :param commit_every: commit archive every n chunks
    :return:

    """
    chunks = split_into_chunks(range(start_poem, start_poem + total_poems), chunk_size)
    ar = Archive('out')
    count = 0
    for chunk in tqdm(chunks, total=len(chunks), unit_scale=chunk_size):
        poems = pool.map(scrape_poem_mp, chunk)
        poems = [p for p in poems if p is not None]
        if verbose:
            print(poems[0]["text"])
        for poem in poems:
            ar.add_data(poem["text"], meta={
                'id': poem["id"],
                'views': poem["views"],
                'comments': poem["comments"],
                'likes': poem["likes"]
            })
        count += 1
        if count == commit_every:
            ar.commit()
            count = 0
    ar.commit()


def read(input_dir_or_file):
    """
    Read a poem from the final jsonl.zst object
    """
    rdr = Reader(input_dir_or_file)
    for doc in os.listdir("out"):
        poem = ""
        for l, meta in rdr.read_jsonl("out/{}".format(doc), get_meta=True):
            poem += l

        print('=====')
        print(meta)
        print(poem)
        print('=====')


def get_new_poem_id():
    """
    Grabs a recent poem ID from allpoetry.com homepage so the scraper knows how many poems to scrape.

    To get the *very latest* poem id you'd need to render JS on the homepage - this method mostly returns poems
    from the same day, so works well enough.

    :return: int, poem_id
    """
    response = requests.get("https://allpoetry.com/#t_newest")
    if response.status_code != 200:
        raise ConnectionError("Response code != 200")
    soup = bs(response.content, "html.parser")
    # items_group t_newest hidden inf
    new = soup.find("div", {"class": re.compile('.*items_group.*')})
    url = int(new.find("a", {"href": re.compile('^/poem/.*')})["href"].replace("/poem/", "").split("-")[0])
    return url


def process_args():
    parser = argparse.ArgumentParser(
        description='CLI for allpoetry dataset - A tool for scraping poems from allpoetry.com')
    parser.add_argument('--latest_id', help='scrape from start_id to latest_id poems (default: 100000)',
                        default=100000,
                        type=int)
    parser.add_argument('--start_id', help='scrape from start_id to latest_id poems (default: 1)',
                        default=1,
                        type=int)
    parser.add_argument('--chunk_size', help='size of multiprocessing chunks (default: 500)',
                            default=1000,
                        type=int)
    parser.add_argument('-a', '--all', action='store_true',
                        help="if this flag is set *all poems* up until the latest poem will be scraped")
    parser.add_argument('-v', '--verbose', action='store_true',
                        help="if this flag is set a poem will be printed out every chunk")
    return parser.parse_args()


if __name__ == "__main__":
    args = process_args()
    if args.all:
        latest_id = get_new_poem_id()
    else:
        latest_id = args.latest_id
    cpu_no = cpu_count()
    p = Pool(cpu_no*6)
    main(latest_id, args.chunk_size, start_poem=args.start_id, pool=p, verbose=args.verbose)
