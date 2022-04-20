import csv

# mongoFile =  open('mongoData.csv', 'r')
# mongo_reader = csv.reader(mongoFile)
# next(mongo_reader, None)

# clarityFile = open('clarity.csv', 'r')
# clarity_reader = csv.reader(clarityFile)
# next(clarity_reader, None)

outputFile = open('missingData.csv', 'w')
output_writer = csv.writer(outputFile)

# i=0
# m_found=False
# for c_row in clarity_reader:
#     # print('{} - {}'.format(c_row[1],c_row[7]))
#     c_row_date = c_row[1]+'.000Z'
#     print('Working on: {}'.format(c_row_date))

#     for m_row in mongo_reader:
#         # print('{} - {}'.format(m_row[3],m_row[1]))
#         if (c_row_date == m_row[3]):
#             print('{} - {} - {} - {} - {}'.format(c_row[1],c_row[7],m_row[1],float(m_row[1])/float(c_row[7]),round(float(c_row[7])*18,0)))
#             m_found=True
#             break
#         else:
#             pass
#     # if (i >= 10):
#     #     break
#     if m_found == False:
#         print(c_row)
#         output_writer.writerow(c_row)
#     else:
#         m_found = False
#     i += 1

m_found = False
with open('clarity-utc.csv', 'r') as clarityFile:
    clarity_reader = csv.reader(clarityFile)
    __ = next(clarity_reader) # Skip header
    for c_row in clarity_reader:
        c_row[7] = 2.17 if c_row[7]=='Low' else c_row[7]
        with open('mongoData.csv', 'r') as mongoFile:
            mongo_reader = csv.reader(mongoFile)
            __ = next(mongo_reader)
            for m_row in mongo_reader:
                if (c_row[14] == m_row[3]):
                    print('{} - {} - {} - {} - {}'.format(c_row[14],c_row[7],m_row[1],float(m_row[1])/float(c_row[7]),round(float(c_row[7])*18,0)))
                    m_found=True
                    break
        if (m_found == False):
            print(c_row)
            output_writer.writerow(c_row)
        else:
            m_found = False

outputFile.close()
