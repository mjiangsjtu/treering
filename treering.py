'''
Copyright (c) 2018 Ming Jiang, Jingchao Li

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
'''

import xlsxwriter
import pandas as pd
from io import StringIO
import time

def GetTableAffected(content):
    #This function parses the table content in DBReplace,
    #and stores non-empty tables in pandas' DataFrame
    content = content.rstrip().split('\n')
    tempDict = {}
    for i in content:
        if i.startswith('TABLE'):
            tablename = i.rstrip().split('\t')[1]
            tempDict[tablename] = ''
        else:
            tempDict[tablename] = tempDict[tablename] + i + '\n'
    nonEmptyTables = [[i, j] for i,j in tempDict.items() if j != '']
    for i in nonEmptyTables:
        #Using StringIO we can use pandas' read_table to convert it to DataFrames
        #as if it was a file
        i[1] = pd.read_table(StringIO(i[1]))
    return [i[0] for i in nonEmptyTables], nonEmptyTables

def GetVariables(tables):
    #This function generates a list of 'tablename.varname' strings
    varnames = []
    for i in tables:
        for j in list(i[1]):
            varnames.append(i[0]+'.'+j)
    return varnames

#Procedure to parser GameSafe data and store them in Python's data structure
def Parser(infile):
    eventList = list()
    previousEvent = -1
    eventID = -1
    questionFlag = False #Two flags to indicate whether we are reading Questionnaire responses
    answerFlag = False
    
    for line in infile:
        if line == '\n': continue
        line = line.split('\t') #gsf files are tab-separated
        #storing questionnaire questions and responses when encountered
        if questionFlag == True and line[0] == '':
            eventList[eventID]['questions'].append(line[1].rstrip())
            continue
        if answerFlag == True and line[0] == '':
            eventList[eventID]['answers'].append(line[1].rstrip())
            continue
        
        if line[0] != '':
            questionFlag = False
            answerFlag = False
            eventID = int(line[0])
            try:
                #Communication between ztree and zleafs are sent in blocks. Each block has an incremental
                #"event id" at the beginning of each line
                #The parsed data are stored in eventList, which is a list of dictionaries
                #Each entry in the list is a block of communication
                if eventID > previousEvent:
                    eventList.append(dict())
                    eventList[eventID]['id'] = eventID
                    previousEvent = eventID
                    eventList[eventID]['event'] = line[1]
                    eventList[eventID][line[2]] = line[3].rstrip()
                    #CGEMS_PGX_DBModify event indicates that one or more of the entry
                    #in one or more of the tables are changed
                    if line[1] == 'CGEMS_PGX_DBModify':
                        #Sometimes more than one table can be changed at the same time (e.g. subjects and contracts)
                        #so DBModify needs to be treated with care
                        eventList[eventID]['tables'] = 0
                        eventList[eventID]['content'] = []
                    #The third [2] entry of each line, if it exists, indicates a keyword 
                    #(e.g. period, subject, time) and the fourth its value
                    eventList[eventID][line[2]] = line[3].rstrip()
                else: #if the next line continues the current event block
                    if line[1] == 'CGEMS_PGX_DBModify':
                        #again, multiple data tables may be changed at once, indicated by multiple
                        #values of m_operation, m_DB, m_recordNrs, and of course, the actual content
                        if line[2] in ['m_operation', 'm_DB']:
                            eventList[eventID].setdefault(line[2], []).append(line[3].rstrip())
                        elif line[2] == 'm_recordNrs':
                            eventList[eventID]['tables'] += 1 #counting how many tables are affected
                            eventList[eventID]['content'].append('')
                            eventList[eventID].setdefault(line[2], []).append([int(i) for i in line[3:]])
                        elif line[2] == '':
                            #the lines for the tables for actual changes start at the third column
                            eventList[eventID]['content'][eventList[eventID]['tables'] - 1] += '\t'.join(line[3:])
                        else:
                            eventList[eventID][line[2]] = line[3].rstrip()
                    #the lines for the tables for actual changes start at the third column
                    elif line[1] != 'CGEMS_PGX_DBModify' and line[2] == '':
                        eventList[eventID]['content'] = eventList[eventID].get('content','') + '\t'.join(line[3:])
                    else:
                        #questionnaire data are stored differently than other data...
                        if line[2] == 'm_questions':
                            eventList[eventID]['questions'] = []
                            eventList[eventID]['questions'].append(line[3].rstrip())
                            questionFlag = True
                        elif line[2] == 'm_answers':
                            eventList[eventID]['answers'] = []
                            eventList[eventID]['answers'].append(line[3].rstrip())
                            answerFlag = True                       
                        else:
                            eventList[eventID][line[2]] = line[3].rstrip()
    
            except IndexError:
                #special case for zleaf connection events
                if line[1].startswith('m_name'):
                    eventList[eventID]['m_name'] = line[1][6:-1]
                elif line[1].startswith('m_IPAddress'):
                    eventList[eventID]['m_IPAddress'] = line[1][11:-1]
                continue
        elif line[1] == 'numSubjects': #another 'special case'
            eventList[eventID]['numSubjects'] = line[2]
    return eventList

def WriteHistory(eventList):
    #The output excel table has four parts:
    #connection events, experiment parameters (e.g. number of subjects and periods)
    #events where data tables are changed (either modified or replaced entirely, for example whenver a constant was defined)
    #and questionnaire responses
    connectList = [i for i in eventList if i['event'] == 'CGESMClientInfo']
    paraList = [i for i in eventList if i['event'] == 'CGEMSParameters']
    dbChangeList = [i for i in eventList if i['event'] in ['CGEMS_PGX_DBReplace', 'CGEMS_PGX_DBModify']]
    workbook = xlsxwriter.Workbook('timeline.xlsx')
    worksheet = workbook.add_worksheet()
    #part 1
    worksheet.write(0, 0, 'time')
    worksheet.write(0, 1, 'client name')
    worksheet.write(0, 2, 'ip address')
    row = 1
    col = 0
    for i in connectList:
        worksheet.write(row, col, i['time'])
        worksheet.write(row, col+1, i['m_name'])
        worksheet.write(row, col+2, i['m_IPAddress'])
        row += 1
    row += 1
    #part 2
    worksheet.write(row, 0, 'parameter name')
    worksheet.write(row, 1, 'value')
    row += 1
    paras = [i.split('\t') for i in paraList[0]['content'].rstrip().split('\n')]
    for i in paras:
        worksheet.write(row, col, i[0])
        worksheet.write(row, col+1, i[1])
        row += 1
    numSubjects = int(paraList[0]['numSubjects'])
    worksheet.write(row, col, 'numSubjects')
    worksheet.write(row, col+1, numSubjects)
    row += 2
    #part 3
    worksheet.write(row, 0, 'Period')
    worksheet.write(row, 1, 'Event ID')
    worksheet.write(row, 2, 'Time')
    worksheet.write(row, 3, 'Event Type')
    worksheet.write(row, 4, 'Subject')
    worksheet.write(row, 5, 'Tables Affected')
    worksheet.write(row, 6, 'Variables Changed')
    row += 1
    for i in dbChangeList:
        if i['event'] == 'CGEMS_PGX_DBReplace':
            if i['target'] == '0':
                tablesAffectedName, tablesAffectedContent = GetTableAffected(i['content'])
                if tablesAffectedName != []:
                    worksheet.write(row, 0, int(i['m_period']) + 1)
                    worksheet.write(row, 1, int(i['id']))
                    worksheet.write(row, 2, i['time'])
                    worksheet.write(row, 3, 'Table replace')
                    worksheet.write(row, 4, 'All')
                    #in the case of tables being replaced, we need to find out:
                    #(1) which tables are being replaced, and
                    #(2) what variables are involved
                    worksheet.write(row, 5, ' '.join(tablesAffectedName))
                    variableName = GetVariables(tablesAffectedContent)
                    for pos, j in enumerate(variableName):
                        worksheet.write(row, 6 + pos, j)
                    row += 1
        elif i['event'] == 'CGEMS_PGX_DBModify':
            worksheet.write(row, 0, int(i['m_period']) + 1)
            worksheet.write(row, 1, int(i['id']))
            worksheet.write(row, 2, i['time'])
            worksheet.write(row, 3, 'Table record modification')
            #in the case of tables being modified, we need to find out:
            #(1) which tables are being modified,
            #(2) what variables are involved,
            #(3) which rows/subjects are affected
            if 'subjects' in i['m_DB']:
                worksheet.write(row, 4, ' '.join([str(x+1) for x in i['m_recordNrs'][i['m_DB'].index('subjects')]]))
            else:
                #tables like contracts do not involve a specific subject
                worksheet.write(row, 4, 'N/A')
            worksheet.write(row, 5, ' '.join(i['m_DB']))
            tables = []
            for j in range(i['tables']):
                tables.append([i['m_DB'][j], pd.read_table(StringIO(i['content'][j]))])
            for pos, j in enumerate(GetVariables(tables)):
                worksheet.write(row, 6 + pos, j)
            row += 1
    row += 1
    #part 4
    quest = [i for i in eventList if i['event'] == 'CGESMQuesterDone']
    if len(quest) > 0:
        for i in quest:
            for idx, j in enumerate(i['questions']):
                worksheet.write(row, idx+1, j)
                worksheet.write(row + 1 + int(i['source']), 0, int(i['source']) + 1)
                worksheet.write(row + 1 + int(i['source']), idx+1, i['answers'][idx])
    workbook.close()
def WriteDataTables(eventList, eventCutoff):
    #Find all events that change data tables (i.e. non-empty DBReplace and DBModify)
    dbReconstruct = [i for i in eventList if ((i['event'] == 'CGEMS_PGX_DBReplace' and i['target'] == '0' and
                                               GetTableAffected(i['content'])[0] != []) or 
                                              (i['event'] == 'CGEMS_PGX_DBModify')) and int(i['id']) <= eventCutoff]
    currentPeriod = int(dbReconstruct[0]['m_period'])  #starting from the earliest period
    dataset = [] #each element in the list for one period
    dataset.append(dict()) #key for table name, value for table content in DataFrame
    for i in dbReconstruct:
        if int(i['m_period']) > currentPeriod:
            dataset.append(dict())
            currentPeriod = int(i['m_period'])
        if i['event'] == 'CGEMS_PGX_DBReplace':
            tablesAffectedName, tablesAffectedContent = GetTableAffected(i['content'])
            for j in tablesAffectedContent:
                #j[0]: table name, j[1]: table content
                if j[0] not in dataset[currentPeriod].keys():
                    dataset[currentPeriod][j[0]] = j[1]
                else:
                    for k in j[1]: #k: column in table
                        dataset[currentPeriod][j[0]][k] = j[1][k]
                if 'Period' not in dataset[currentPeriod][j[0]]:
                    dataset[currentPeriod][j[0]]['Period'] = str(currentPeriod + 1)
        elif i['event'] == 'CGEMS_PGX_DBModify':
            for t in range(i['tables']): #multple tables may be changed in one event
                #separate table header (modTable[0]) from table rows (modTable[1:])
                modTable = [k.split('\t') for k in i['content'][t].rstrip().split('\n')]
                for j in range(len(modTable[0])): #j: index of variable names
                #m_DB: tables changed; m_recordNrs: rows/subjects affected
                    if i['m_DB'][t] == 'session':
                        recordNrs = [int(i['target'])]
                    else:
                        recordNrs = i['m_recordNrs'][t]
                    for idx, k in enumerate(recordNrs): #rows/subjects affected
                        if i['m_DB'][t] not in dataset[currentPeriod]:
                            dataset[currentPeriod][i['m_DB'][t]] = pd.DataFrame()
                        dataset[currentPeriod][i['m_DB'][t]].loc[k, modTable[0][j]] = modTable[idx+1][j]
                        dataset[currentPeriod][i['m_DB'][t]].loc[k, 'Period'] = str(currentPeriod + 1)
    dataset2 = dict() #create a new dictionary to merge tables in separate periods into different tables
    for i in dataset:
        for k, v in i.items():
            if k not in dataset2.keys():
                dataset2[k] = v
            else:
                dataset2[k] = dataset2[k].append(v, ignore_index=False, sort=True)
    #Finally, write to tab-separated text files
    for k, v in dataset2.items():
        v.to_csv(str(eventCutoff) + '_' + k + '.txt', sep='\t')

print("TreeRing: a GameSafe Parser for zTree")

while True:
    try:
        filename = input("Please enter the file name of the exported readable GameSafe text file:\n")
        infile = open(filename, 'r')
        break
    except FileNotFoundError:
        print('The file does not exist. Please check your input and try again.')

print('Parsing the GameSafe file, please wait...')

start_time = time.time()
eventList = Parser(infile)
print("--- %s seconds ---" % (time.time() - start_time))

print('Done!')
print('Now writing experimental history to file...')

start_time = time.time()
WriteHistory(eventList)
print("--- %s seconds ---" % (time.time() - start_time))

print('Done! The experimental history has been written in the timeline.xlsx file.')
print('What do you want to do next?')
print('    Type in the Event ID to reconstruct the data tables up to this point.')
print('    Type \'end\' to reconstruct the data tables at the end of the experiment.')
print('    Type \'all\' to reconstruct the data tables whenever')   
print('      whenever there is a change to the database')
print('      (Warning: A lot files will be created!)')

while True:
    choice = input('Please make your choice:\n')
    if choice == 'end':
        print('Writing data tables, please wait...')
        eventCutoff = len(eventList)
        start_time = time.time()
        WriteDataTables(eventList, eventCutoff)
        print("--- %s seconds ---" % (time.time() - start_time))
        break
    elif choice == 'all':
        print('Writing data tables, please wait...')
        #in this case we need all event cutoffs...
        eventCutoffs = [int(i['id']) for i in eventList if ((i['event'] == 'CGEMS_PGX_DBReplace' and i['target'] == '0' and
                        GetTableAffected(i['content'])[0] != []) or (i['event'] == 'CGEMS_PGX_DBModify'))]
        for i in eventCutoffs:
            WriteDataTables(eventList, i)
        break
    else:
        try:
            eventCutoff = int(choice)
            WriteDataTables(eventList, eventCutoff)
            break
        except:
            print('Please make a valid choice.')

print('Done!')
input("Press any key to continue...")




