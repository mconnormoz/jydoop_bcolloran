import json
import jydoop
import random



'''
approx 450*10^6 records in full HBASE,
so 1% sample has 4.5*10^6,
to get ~4500 record sample, sample 10^-3 of the 1%,
i.e. (4.5*10^6)*10^-3 = 4.5*10^3 = 4500
'''

"""
make ARGS="scripts/fhrSampling/getSample_v2_from1pct.py outData/samples/fhrSample_v2records_2014-03-25.tsv /user/sguha/fhr/samples/output/1pct" hadoop"""

sampleRate = 0.001

setupjob = healthreportutils.setupjob


def map(fhrDocId, rawJsonIn, context):

    if random.random()<=sampleRate:
        context.write(fhrDocId, rawJsonIn)





