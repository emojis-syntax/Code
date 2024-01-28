#!/usr/bin/python3


from __future__ import unicode_literals
import re
import os
import sys

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

def generate_emoji_list(emoji_source):
    emoji_raw = load_current_emoji(emoji_source)
    #EmojiEntry = namedtuple('EmojiEntry', ['codepoint', 'status', 'emoji', 'name', 'group', 'sub_group'])
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
            #templine = EmojiEntry(codepoint=codepoint, status=status, emoji=emoji,  name=name, group=group, sub_group=subgroup)
            data = {}
            data['emoji'] = emoji
            data['name'] = name
            emoji_entries.append(data)

    #emoji_dict = {x.emoji: x for x in emoji_entries}
    #multi_codepoint_emoji = []

    return emoji_entries


emojis = generate_emoji_list(emoji_source)
print(emojis)


