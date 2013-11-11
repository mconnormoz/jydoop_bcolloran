import jydoop

'''
in following commands, UPDATE DATES

make ARGS="scripts/dumpFhrDocsByDocId.py ./outData/PATH_ON_PEACH_TO_DUMP_TO ./PATH_ON_HDFS_TO_DUMP" hadoop

'''


setupjob = jydoop.setupjob


docIdList = ['000c7f20-56e6-4120-b469-4012408c2150', '00f3d366-7bf0-40c9-bb0e-e633124ad6bc', '01fc91a8-2f1d-49bc-962e-817fb035fd24', '0ae1911f-23e4-4da8-aff3-7b07ba33dcc2', '183e92ae-acd8-44de-ab6e-b0124afeaf16', '1cc99108-ce7a-4bdb-903d-0de2c5fb1483', '224339a8-34e3-42c4-9dc2-d3491f6c5540', '28556838-aed2-4ca0-97e6-bde63ffa2f99', '656e8d20-59b8-48bb-a2ef-761336cea954', '722c3f45-c5ef-4804-bad9-a136e2a21b2c', '792a8c43-0009-4645-a2bd-8a0f85df2d91', '84a5f19b-7798-4db8-846e-8ba44e9b0181', '8ea3cc2d-05a1-4ee8-86f5-cc5231181bd6', '954ce4f6-d855-4652-b523-51ee583bc0c2', '985a2fda-8383-4d1b-8ab4-57745f5668da', '98ae16a0-628c-4414-b2cf-717a6aa25598', '99c9a20f-0706-404e-b955-af8a101ffbd4', 'a578cc13-fd26-429e-a37e-dafdba2b8e8f', 'a81b7b1a-649e-4315-aac2-a97fa93e0399', 'af2391c3-8b85-4a59-ab2a-efbbe4276a6f', 'b139d7c7-60c2-42c6-8d95-26a7fc54e02a', 'e3acee3e-c295-4339-aef9-8e231c3eb922', 'ec5eda00-a91e-41a8-95d1-0e1dd1ae1013', 'ee68e143-6152-4114-95db-c8f59b3c5961', 'f7f667f0-c865-4e3d-a44c-22a65eb90f31']



def output(path, results):
    # just dump tab separated key/vals
    f = open(path, 'w')
    for k, v in results:
        print >>f, str(v)



def map(key,val,context):
    if key in docIdList:
        context.write(str(key),str(val))