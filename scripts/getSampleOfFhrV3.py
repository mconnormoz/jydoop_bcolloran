import json
import jydoop
import healthreportutils
import random




sampleRate = 0.0001

setupjob = healthreportutils.setupjob


def map(fhrDocId, rawJsonIn, context):

    try:
        payload = json.loads(rawJsonIn)
    except KeyError:
        return

    try: #was getting errors finding packets without a version field, so had to wrap this test in a try block
        if not (payload["version"]==3):
            return
    except KeyError:
        return

    if random.random()<=sampleRate:
        context.write(fhrDocId, rawJsonIn)





