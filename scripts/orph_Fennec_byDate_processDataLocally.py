import jydoop
import healthreportutils_v3


'''
a job to locally process the output of orph_Fennec_byDate.py
run:

jydoopLocal -t /home/bcolloran/Desktop/projects/fhr/orphanDetection/data/orphaningDatesFennec_2013-10-22.csv -o /home/bcolloran/Desktop/projects/fhr/orphanDetection/data/orphaningFennec_byDate_2013-10-22.csv -v scripts/orph_Fennec_byDate_processDataLocally.py 

'''


setupjob = healthreportutils_v3.setupjob




def map(key, line, context):
    for date in line.strip().split(",")[1:]:
       context.write(date,1)


combine = jydoop.sumreducer
reduce = jydoop.sumreducer







