import csv
import io
# def unicode_csv_reader(utf8_data, **kwargs):
#         csv_reader = csv.reader(utf8_data,  **kwargs)
#         for row in csv_reader:
#             yield [unicode(cell, 'utf-8') for cell in row]
#
def file_reader_simple_csv(fil,delim=',',is_unicode=False):
    f = open(fil)
    # if is_unicode:
    #     reader = csv.reader(io.open(fil),delimiter=delim)
    # else:
    reader = csv.reader(f, delimiter=delim)
    vars = reader.next()
    return reader


def get_party(party_text, party_map):
    party = party_text.strip()
    if len(party_text):
        party = 'O' if party_text not in party_map else party_map[party_text]
    return party
