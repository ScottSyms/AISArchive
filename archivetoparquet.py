import pandas as pd
from bitstring import BitString
import pyarrow
import pyarrow.parquet as pq
import numpy as np

import sys

# Constants
lookupstring = "@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_ !\"#$%&\\()*+,-./0123456789:;<=>?"


def convertPayload(string):
    """
    AIS payloads are encoded in six bit ascii.  This converts the payload into
    ASCII for further processing.
    """
    binword = ''
    for piece in str(string):
        # Subtract 48 from the ascii value.
        # If the value is greater than 40, subtract 8
        ascii_piece = ord(piece) - 48
        if ascii_piece > 40:
            ascii_piece = ascii_piece - 8

        # to convert the string to binary.
        for x in [32, 16, 8, 4, 2, 1]:
            if ascii_piece - x >= 0:
                ascii_piece = ascii_piece - x
                binword = binword + '1'
            else:
                binword = binword + '0'
    return binword


def getMessageType(row):
    return BitString(bin=row['binpayload'])[0:6].uint


def getMMSI(row):
    # if row['messagetype'] in [1, 2, 3, 18, 24]:
    return str(BitString(bin=row['binpayload'])[8:38].uint).zfill(9)
    # else:
    #    return None


def getLongitude(row):
    payload = BitString(bin=row['binpayload'])
    payload = payload + \
        BitString('0x000000000000000000000000000000000000000000000000000')
    if row['messagetype'] in [1, 2, 3, 9]:
        longitude = float(payload[61:89].int)/600000
    elif row['messagetype'] in [4]:
        longitude = float(payload[79:107].int) / 600000
    elif row['messagetype'] in [18, 19]:
        longitude = float(payload[57:85].int) / 600000
    elif row['messagetype'] in [21]:
        longitude = float(payload[164:192].int) / 600000
    elif row['messagetype'] in [27]:
        longitude = float(payload[44:62].int) / 600000
    else:
        longitude = None
    return longitude


def getLatitude(row):
    payload = BitString(bin=row['binpayload'])
    payload = payload + \
        BitString('0x100000000000000000000000000000000000000000000000000')
    if row['messagetype'] in [1, 2, 3, 9]:
        latitude = float(payload[89:116].int)/600000
    elif row['messagetype'] in [4]:
        latitude = float(payload[107:134].int) / 600000
    elif row['messagetype'] in [18, 19]:
        latitude = float(payload[85:112].int) / 600000
    elif row['messagetype'] in [21]:
        latitude = float(payload[192:219].int) / 600000
    elif row['messagetype'] in [27]:
        latitude = float(payload[62:79].int) / 600000
    else:
        latitude = None
    return latitude


def returnAscii(x):
    ascii_piece = x + 48
    if ascii_piece > 31:
        ascii_piece = ascii_piece + 8
    return chr(ascii_piece)


def clean(data):
    """
    Make sure the text is allowable
    """
    text = ''
    for i in data.upper():
        if i in lookupstring:
            text = text + i
    return text


def converttoString(payload):
    '''
    Convert from sixbit ascii to text

    '''
    lookupstring = "@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_ !\"#$%&\\()*+,-./0123456789:;<=>?"
    length = len(payload)
    payload = BitString(
        bin=payload) + BitString('0x00000000000000000000000000000000000000000000000000000')
    word = ''
    while True:
        try:
            snip = payload.read("uint:6")
        except:
            return clean(word.replace('@', '').strip())
        if snip == '' or payload.pos == length:
            return clean(word.replace('@', '').strip())
        else:
            word = word + lookupstring[snip]


def getDestination(payload):
    return converttoString(payload[302:422])


def getShipname(payload):
    return converttoString(payload[112:232])


def getCallsign(payload):
    return converttoString(payload[70:112])


print("\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
print("Reading the data from ", sys.argv[2])
df = pd.read_csv(sys.argv[2], delimiter='!',
                 header=0, names=['prefix', 'suffix'])

print("Extracting the payload")
df['payload'] = df.suffix.str.extract(
    r'AIVDM,\w*,\w*,\w*,\w*,([A-Za-z0-9:;<=>=?@`]+)')

print("Converting the payload to binary")
df['binpayload'] = df.payload.apply(convertPayload)


print("Getting the message type")
df['messagetype'] = df.apply(lambda x: getMessageType(x), axis=1)

print("Getting the MMSI")
df['mmsi'] = df.apply(lambda x: getMMSI(x), axis=1)


print("Getting the longitude and latitude")
df['longitude'] = df.apply(lambda x: getLongitude(x), axis=1)
df['latitude'] = df.apply(lambda x: getLatitude(x), axis=1)

print("extracting the received and reported time")
try:
    df['received_time'] = df.prefix.str.extract(r'(^[\d]*)')
    df['received_time'] = df.received_time.apply(
        lambda x: pd.Timestamp(int(x), unit='s', tz="UTC"))
except:
    pass

df['report_time'] = df.prefix.str.extract(r'c:(\d+)')
df['report_time'] = df.report_time.apply(lambda x: x if pd.isna(
    x) else pd.Timestamp(int(x), unit='s', tz="UTC"))
value = pd.Timestamp(0, unit='s', tz='UTC')
df.report_time.fillna(value, inplace=True)

print("Extracting sentence, group, fragment data and padding value")
df['source'] = df.prefix.str.extract(r's:(\w*)')
df['group'] = df.prefix.str.extract(r'g:([A-Za-z0-9-]+)')
df['fragments'] = df.suffix.str.extract(r'AIVDM,(\w*)')
df['fragment'] = df.suffix.str.extract(r'AIVDM,\w*,(\w*)')
df['fragmentid'] = df.suffix.str.extract(r'AIVDM,\w*,\w*,(\w*)')
df['frequency'] = df.suffix.str.extract(r'AIVDM,\w*,\w*,\w*,(\w*)')
df['padding'] = df.suffix.str.extract(r',(\d+)\*')

print("Appending second fragment")
df['merge'] = df['mmsi'] + df['group']
seconds = df.query("fragments == '2' and fragment=='2'")[
    ['merge', 'binpayload']]
df = df.merge(seconds, on='merge',  how='outer')
df['binpayload'] = np.where(
    pd.isna(df.binpayload_y), df.binpayload_x, df.binpayload_x + df.binpayload_y)

print("Removing second fragment")
df.drop(df[df.fragment == '2'].index, inplace=True)

print("Adding destination")
df['destination'] = df.query("messagetype == '5'").binpayload.apply(
    lambda x: getDestination(x))

print("Adding callsign")
df['callsign'] = df.query("messagetype == '5'").binpayload.apply(
    lambda x: getCallsign(x))

print("Adding adding ship name")
df['shipname'] = df.query("messagetype == '5'").binpayload.apply(
    lambda x: getShipname(x))

print('Create date values to partition on')
df['year'] = df.report_time.apply(lambda x: int(x.year))
df['month'] = df.report_time.apply(lambda x: int(x.month))
df['day'] = df.report_time.apply(lambda x: int(x.day))
df['hour'] = df.report_time.apply(lambda x: int(x.hour))

print("Drop columns and save file")
df.drop(columns=['prefix', 'suffix', 'payload',
                 'binpayload_x', 'binpayload_y'], inplace=True)
table = pyarrow.Table.from_pandas(df)
pq.write_to_dataset(table, sys.argv[1], partition_cols=[
                    'year', 'month', 'day', 'hour'], compression='snappy')
