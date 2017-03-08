import string
from twitter_dm.nlp.nlp_helpers import get_cleaned_text
from nameparser import HumanName
import io
import re
from twitter_dm.utility.general_utils import tab_stringify_newline


def stringify(data):
    return [unicode(x).replace(u"\r\n",u"   ")
                      .replace(u"\r",u"   ")
                      .replace(u"\n",u"   ")
                      .replace(u"\t", u"   ") for x in data]

def tsn(data,newline=True):
    to_return = "\t".join(stringify(data))
    if newline:
         return to_return + "\n"
    return to_return


def clean_name_text(orig_name):
    # clean any name field
    name2 = re.sub(r'[^\x00-\x7F]+',' ', orig_name)
    name3 = get_cleaned_text(name2)
    name3 = name3.replace(".","")
    return name3

def clean_name(orig_name):
    # get the first and last name
    cleaned_name = clean_name_text(orig_name)
    try:
        n = HumanName(cleaned_name)
    except IndexError:
        return None, None, None
    last = n.last.lower()
    first = n.first.lower()
    return first, last, cleaned_name

def gen_first_middle_last_name(d):
    if len(d.split()) < 2 or len(d.split()) > 4:
        return None, None, None
    first, middle, last = clean_name(d)

    if not first or len(first) < 2:
        return None, None, None
    return first, middle, last

def write_file(res,filename):
    outfil = io.open(filename,"w")
    outfil.write(tab_stringify_newline(res.columns.tolist()))
    for v in res.itertuples():
        try:
            outfil.write(tab_stringify_newline(v[1:]))
        except:
            print 'fail - something weird with address',v
    outfil.close()

# hacky but necessary for now

def get_florida_specific_location_name(k):
    k = re.sub("^ft ","fort ", k)
    k = re.sub("^st ","saint ", k)
    k = re.sub("^mt ","mount ", k)
    k = re.sub("^mt ","mount ", k)

    if k == "ft myers":
        k = "fort myers"
    elif k == "fort lauderdale":
        k = "ft lauderdale"
    elif k == "tampa bay":
        k = "tampa"
    elif k == 'port st lucie':
        k = 'pt st lucie'
    elif k == "jax":
        k = 'jacksonville'
    elif k == "saint petersburg":
        k = 'st petersburg'
    return k

def get_colorado_specific_location_name(k):
    if k == 'colorado springs':
        k = 'colo springs'
    return k

