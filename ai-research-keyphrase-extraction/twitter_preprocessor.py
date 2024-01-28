from __future__ import unicode_literals
import pandas as pd
import launch
import re
import time
import sys
from collections import namedtuple, Counter

import argparse
parser = argparse.ArgumentParser()
#parser.add_argument("-m", "--model", default="twitter_bigrams.bin", help="Model filename")
parser.add_argument("-d", "--dataset", required=True, help="Dataset filename")
args = parser.parse_args()
dataset = args.dataset

# emoji extraction
# code adapted. credit to Elias Dabbas
# https://www.kaggle.com/eliasdabbas/how-to-create-a-python-regex-to-extract-emoji

emoji_source = './emoji-test.txt'


def load_current_emoji(emoji_source):
    try:
        with open(emoji_source, 'rt') as file:
            emoji_raw = file.read()
            return emoji_raw
    except OSError:
        print('Could not open file', emoji_source)
        return -1
    else:
        print('Something went wrong')


def get_ranges(nums):
    """Reduce a list of integers to tuples of local maximums and minimums.

    :param nums: List of integers.
    :return ranges: List of tuples showing local minimums and maximums
    """
    nums = sorted(nums)
    lows = [nums[0]]
    highs = []
    if nums[1] - nums[0] > 1:
        highs.append(nums[0])
    for i in range(1, len(nums) - 1):
        if (nums[i] - nums[i - 1]) > 1:
            lows.append(nums[i])
        if (nums[i + 1] - nums[i]) > 1:
            highs.append(nums[i])
    highs.append(nums[-1])
    if len(highs) > len(lows):
        lows.append(highs[-1])
    return [(l, h) for l, h in zip(lows, highs)]


def generate_emoji_regex(emoji_source):
    emoji_raw = load_current_emoji(emoji_source)
    EmojiEntry = namedtuple('EmojiEntry', ['codepoint', 'status', 'emoji', 'name', 'group', 'sub_group'])
    E_regex = re.compile(r' ?E\d+\.\d+ ')  # remove the pattern E<digit(s)>.<digit(s)>
    emoji_entries = []

    for line in emoji_raw.splitlines()[32:]:  # skip the explanation lines
        if line == '# Status Counts':  # the last line in the document
            break
        if 'subtotal:' in line:  # these are lines showing statistics about each group, not needed
            continue
        if not line:  # if it's a blank line
            continue
        if line.startswith('#'):  # these lines contain group and/or sub-group names
            if '# group:' in line:
                group = line.split(':')[-1].strip()
            if '# subgroup:' in line:
                subgroup = line.split(':')[-1].strip()
        if group == 'Component':  # skin tones, and hair types, skip, as mentioned above
            continue  # maybe this continue is because of greedy regex or because compound emoji have it already
        if re.search('^[0-9A-F]{3,}', line):  # if the line starts with a hexadecimal number (an emoji code point)
            # here we define all the elements that will go into emoji entries
            codepoint = line.split(';')[0].strip()  # in some cases it is one and in others multiple code points
            status = line.split(';')[-1].split()[0].strip()  # status: fully-qualified, minimally-qualified, unqualified
            if line[-1] == '#':
                # The special case where the emoji is actually the hash sign "#". In this case manually assign the emoji
                if 'fully-qualified' in line:
                    emoji = '#️⃣'
                else:
                    emoji = '#⃣'  # they look the same, but are actually different
            else:  # the default case
                emoji = line.split('#')[-1].split()[0].strip()  # the emoji character itself
            if line[-1] == '#':  # (the special case)
                name = '#'
            else:  # extract the emoji name
                split_hash = line.split('#')[1]
                rm_capital_E = E_regex.split(split_hash)[1]
                name = rm_capital_E
            templine = EmojiEntry(codepoint=codepoint,
                                  status=status,
                                  emoji=emoji,
                                  name=name,
                                  group=group,
                                  sub_group=subgroup)
            emoji_entries.append(templine)

    emoji_dict = {x.emoji: x for x in emoji_entries}
    multi_codepoint_emoji = []

    for code in [c.codepoint.split() for c in emoji_entries]:
        if len(code) > 1:
            # turn to a hexadecimal number zfilled to 8 zeros e.g: '\U0001F44D'
            hexified_codes = [r'\U' + x.zfill(8) for x in code]
            hexified_codes = ''.join(hexified_codes)  # join all hexadecimal components
            multi_codepoint_emoji.append(hexified_codes)

    # sorting by length in decreasing order is extremely important as demonstrated above
    multi_codepoint_emoji_sorted = sorted(multi_codepoint_emoji, key=len, reverse=True)

    # join with a "|" to function as an "or" in the regex
    multi_codepoint_emoji_joined = '|'.join(multi_codepoint_emoji_sorted)
    multi_codepoint_emoji_joined[:400]  # sample

    single_codepoint_emoji = []

    for code in [c.codepoint.split() for c in emoji_entries]:
        if len(code) == 1:
            single_codepoint_emoji.append(code[0])
    # We first convert single_codepoint_emoji to integers to make calculations easier
    single_codepoint_emoji_int = [int(x, base=16) for x in single_codepoint_emoji]
    single_codepoint_emoji_ranges = get_ranges(single_codepoint_emoji_int)
    single_codepoint_emoji_raw = r''  # start with an empty raw string
    for code in single_codepoint_emoji_ranges:
        if code[0] == code[1]:  # in this case make it a single hexadecimal character
            temp_regex = r'\U' + hex(code[0])[2:].zfill(8)
            single_codepoint_emoji_raw += temp_regex
        else:
            # otherwise create a character range, joined by '-'
            temp_regex = '-'.join([r'\U' + hex(code[0])[2:].zfill(8), r'\U' + hex(code[1])[2:].zfill(8)])
            single_codepoint_emoji_raw += temp_regex
    all_emoji_regex = re.compile(multi_codepoint_emoji_joined + '|' + r'[' + single_codepoint_emoji_raw + r']')

    return all_emoji_regex


def load_tweet_json_to_pd(tweet_source):
    """
    This function receives the string path to the json tweets source to be processed
    tweet_source should be something like "./data/raw/filename.json"
    """
    try:
        tweets_df = pd.read_json(tweet_source)
        return tweets_df
    except OSError:
        print('Could not open file', emoji_source)
        return -1
    else:
        print('Something went wrong')
        return -1


def preprocess_emoji(tweets_df, emoji_regex):
    """
    Find emoji in text columns and add columns to contain them separately
    emoji_regex must have been computed previously and passed as argument
    """

    # extract emoji from the text columns
    t_emoji = tweets_df['text'].str.findall(emoji_regex.pattern)
    r_emoji = tweets_df['rtext'].str.findall(emoji_regex.pattern)
    q_emoji = tweets_df['qtext'].str.findall(emoji_regex.pattern)

    tweets_df['text_emoji'] = t_emoji
    tweets_df['rtext_emoji'] = r_emoji
    tweets_df['qtext_emoji'] = q_emoji

    return tweets_df


def preprocess_hashtags(tweets_df, hashtag_regex):
    """
    Find hashtags in text columns and add columns to contain them separately
    hashtag_regex must have been defined previously and passed as argument
    """

    # extract emoji from the text columns
    t_hashtag = tweets_df['text'].str.findall(hashtag_regex)
    r_hashtag = tweets_df['rtext'].str.findall(hashtag_regex)
    q_hashtag = tweets_df['qtext'].str.findall(hashtag_regex)

    tweets_df['text_hashtag'] = t_hashtag
    tweets_df['rtext_hashtag'] = r_hashtag
    tweets_df['qtext_hashtag'] = q_hashtag

    return tweets_df


def preprocess_mentions(tweets_df, mention_regex):
    """
    Find mentions in text columns and add columns to contain them separately
    mention_regex must have been defined previously and passed as argument
    """
    # extract emoji from the text columns
    t_mention = tweets_df['text'].str.findall(mention_regex)
    r_mention = tweets_df['rtext'].str.findall(mention_regex)
    q_mention = tweets_df['qtext'].str.findall(mention_regex)

    tweets_df['text_mention'] = t_mention
    tweets_df['rtext_mention'] = r_mention
    tweets_df['qtext_mention'] = q_mention

    return tweets_df


def preprocess_symbols(tweets_df, symbol_regex):
    """
    Find mentions in text columns and add columns to contain them separately
    symbol_regex must have been defined previously and passed as argument
    """
    # extract emoji from the text columns
    t_symbol = tweets_df['text'].str.findall(symbol_regex)
    r_symbol = tweets_df['rtext'].str.findall(symbol_regex)
    q_symbol = tweets_df['qtext'].str.findall(symbol_regex)

    tweets_df['text_symbol'] = t_symbol
    tweets_df['rtext_symbol'] = r_symbol
    tweets_df['qtext_symbol'] = q_symbol

    return tweets_df


def preprocess_urls(tweets_df, url_regex):
    """
    Find mentions in text columns and add columns to contain them separately
    url_regex must have been defined previously and passed as argument
    """
    # extract emoji from the text columns
    t_url = tweets_df['text'].str.findall(url_regex)
    r_url = tweets_df['rtext'].str.findall(url_regex)
    q_url = tweets_df['qtext'].str.findall(url_regex)

    tweets_df['text_url'] = t_url
    tweets_df['rtext_url'] = r_url
    tweets_df['qtext_url'] = q_url

    return tweets_df


def extract_keyphrases(tweets_df, threshold):
    """
    Must have started stanford core nlp server
    Must have imported launch
    TO-DO make sure script does this beforehand
    """
    return tweets_df


# 'mini_test_set.json'
# 'emoji-test.txt'

def produce_json_output(tweet_source, emoji_source):
    """
    This function writes json to repository.
    This is the mother function
    """

    emoji_regex = generate_emoji_regex(emoji_source)
    url_regex = r"""(?i)\b((?:https?:(?:/{1,3}|[\w0-9%])|[\w0-9.\-]+[.](?:\w{2,63})/)(?:[^\s()<>{}\[\]]+|\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\))+(?:\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\)|[^\s`!()\[\]{};:'".,<>?«»“”‘’])|(?:\b(?<![@.])[\w0-9]+(?:[.\-][\w0-9]+)*[.](?:\w{2,63})\b/?(?!@)))"""
    mention_regex = r'(?i)(?<!\w\@)((?<=\@)\w+)'
    hashtag_regex = r'(?i)(?<=\#)\w+'
    symbol_regex = r'(?i)(?<=\$)\w+'

    tweets_df = load_tweet_json_to_pd(tweet_source)

    tweets_df['clean_text'] = tweets_df['text']
    tweets_df['clean_rtext'] = tweets_df['rtext']
    tweets_df['clean_qtext'] = tweets_df['qtext']

    tweets_df = preprocess_emoji(tweets_df, emoji_regex)
    tweets_df = preprocess_hashtags(tweets_df, hashtag_regex)
    tweets_df = preprocess_mentions(tweets_df, mention_regex)
    tweets_df = preprocess_symbols(tweets_df, symbol_regex)
    tweets_df = preprocess_urls(tweets_df, url_regex)

    t_clean = ((((tweets_df['text'].str.replace(emoji_regex.pattern, '')).str.replace(mention_regex, '')).str.replace(
        url_regex, '')).str.replace('@|#', '')).str.replace(symbol_regex, '')
    tweets_df['clean_text'] = t_clean
    r_clean = ((((tweets_df['rtext'].str.replace(emoji_regex.pattern, '')).str.replace(mention_regex, '')).str.replace(
        url_regex, '')).str.replace('@|#', '')).str.replace(symbol_regex, '')
    tweets_df['clean_rtext'] = r_clean
    q_clean = ((((tweets_df['qtext'].str.replace(emoji_regex.pattern, '')).str.replace(mention_regex, '')).str.replace(
        url_regex, '')).str.replace('@|#', '')).str.replace(symbol_regex, '')
    tweets_df['clean_qtext'] = q_clean

    # TO-Do serialise for json dump
    tweets_df = extract_keyphrases(tweets_df)
    #tweets_df.to_json("out.json")
    print(tweets_df.to_json())


def my_kpextraction(tx):
    if tx != '':
        return launch.extract_keyphrases(embedding_distributor, pos_tagger, tx, 10, 'en')
    else:
        return []

def extract_keyphrases(df):
    """
    Must have started stanford core nlp server
    Must have imported launch
    TO-DO make sure script does this beforehand
    """

    kpt = df['clean_text'].apply(my_kpextraction)
    kpr = df['clean_rtext'].apply(my_kpextraction)
    kpq = df['clean_qtext'].apply(my_kpextraction)
    df['text.keyphrases'] = kpt
    df['rtext.keyphrases'] = kpr
    df['qtext.keyphrases'] = kpq

    return df

t0 = time.time()
#print('launch imported')
embedding_distributor = launch.load_local_embedding_distributor()
#print('embedding imported')
pos_tagger = launch.load_local_corenlp_pos_tagger()
#mydf = produce_json_output('tweet_set.json', 'emoji-test.txt')
mydf = produce_json_output(dataset, 'emoji-test.txt')
t1 = time.time()

total = t1-t0
print("DELTA: " + str(total), file=sys.stderr)
