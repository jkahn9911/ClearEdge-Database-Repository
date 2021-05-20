import pandas as pd
import numpy as np
from simple_salesforce import Salesforce
import configFile

def createSalesforceConnection():
    return Salesforce(username=configFile.sfUserName, password=configFile.sfPassword, security_token='7BWxNmjwYkB1Qqw6qFwCXd5AI')

def createCaseDF(sfConnection):
    casesQuery = sfConnection.query_all("SELECT Id, CaseNumber, Sharepoint_Library_YearQtr__c FROM Case WHERE IsDeleted = FALSE")
    id = []
    number = []
    spLibrary = []
    for case in casesQuery['records']:
        id.append(case['Id'])
        number.append(str(int(case['CaseNumber'])))
        spLibrary.append(case['Sharepoint_Library_YearQtr__c'])

    caseData = {'Case ID' : id, 'Case #' : number, 'SP Library' : spLibrary}
    return pd.DataFrame(caseData)

def createSupplierDF(sfConnection):
    supplierQuery = sfConnection.query_all("SELECT Id, Name FROM Supplier__c WHERE IsDeleted = FALSE")
    id = []
    name = []
    for supplier in supplierQuery['records']:
        id.append(supplier['Id'])
        name.append(supplier['Name'].upper())

    supplierData = {'Supplier ID' : id, 'Service Provider' : name}

    return pd.DataFrame(supplierData)

def createObjectxDF(sfConnection):
    objXQuery = sfConnection.query_all("SELECT Case_Number__c, CreatedDate, Id, Practice_Single__c, Primary__c, Round__c, Supplier__c FROM Object_X__c WHERE IsDeleted = FALSE")
    caseNumber = []
    createdDate = []
    id = []
    practice = []
    primary = []
    round = []
    supplier = []
    for objX in objXQuery['records']:
        if objX['Practice_Single__c'] in ['Managed Services', 'Third Party Labor', 'Professional Services']:
            caseNumber.append(str(int(objX['Case_Number__c'])))
            createdDate.append(objX['CreatedDate'])
            id.append(objX['Id'])
            practice.append(objX['Practice_Single__c'])
            primary.append(objX['Primary__c'])
            round.append(objX['Round__c'])
            supplier.append(objX['Supplier__c'])

    objxData = {'Case #' : caseNumber, 'Obj X Created Date' : createdDate, 'ID' : id,'Practice' : practice, 'Primary' : primary, 'Round' : round, 'Supplier ID' : supplier}
    objectxs = pd.DataFrame(objxData)
    return objectxs

def setLatestDateCount(supplierId, caseNum, isPrimary, curRound, createdDate, objectxsCopy):
    sameSupplier = objectxsCopy[objectxsCopy['Supplier ID'] == supplierId]
    sameCase = sameSupplier[sameSupplier['Case #'] == caseNum]
    count = len(sameCase.index)
    if count == 1:
        return 1
    if count > 1:
        otherPrimary = sameCase[sameCase['Primary']]
        countP = len(otherPrimary.index)
        if countP == 1 and isPrimary:
            return 1
        if (countP == 0 and (not isPrimary)) or (countP > 1 and (isPrimary)):
            higherRounds = sameCase[sameCase['Round'] > curRound]
            countH = 0
            sameRound = sameCase[sameCase['Round'] == curRound]
            if len(higherRounds.index) == 0: 
                countH = len(sameRound.index)
                if countH == 1:
                    return 1
                if countH > 1:
                    laterDates = sameRound[sameRound['Obj X Created Date'] > createdDate]
                    countD = 0
                    if len(laterDates.index) == 0:
                        sameDate = sameRound[sameRound['Obj X Created Date'] == createdDate]
                        countD = len(sameDate.index)
                    return countD
        return 0

def mergeAndFilterObjXs(objxs, suppliers, cases):
    prevObjectxs = pd.read_csv('prevObjectxs.csv').drop('Unnamed: 0', 1)
    prevObjectxs['Case #'] = prevObjectxs['Case #'].astype(str)
    objectxsJoined = prevObjectxs.merge(right=objxs, how='right', indicator=True)
    newObjectxCases = objectxsJoined[objectxsJoined['_merge'] == 'right_only']['Case #'].unique()
    newObjectxs = objectxsJoined[objectxsJoined['Case #'].isin(newObjectxCases)]

    if len(newObjectxs) > 0:
        newObjectxs = newObjectxs.merge(suppliers, on='Supplier ID')
        newObjectxs = newObjectxs.merge(cases, on='Case #')
        newObjectxs['Count - Latest Date'] = newObjectxs.apply(lambda row: setLatestDateCount(row['Supplier ID'], row['Case #'], row['Primary'], row['Round'], row['Obj X Created Date'], newObjectxs.copy()), axis=1)
        newObjectxs = newObjectxs[newObjectxs['Count - Latest Date'] == 1].drop('Count - Latest Date', 1)

    prevObjectxsFiltered = pd.read_csv('prevObjectxsFiltered.csv').drop('Unnamed: 0', 1)
    prevObjectxsFiltered['Case #'] = prevObjectxsFiltered['Case #'].astype(str)
    prevObjectxsFiltered = prevObjectxsFiltered[~prevObjectxsFiltered['Case #'].isin(newObjectxCases)]
    objectxsFiltered = newObjectxs.append(prevObjectxsFiltered).drop('_merge',1)
    return objectxsFiltered

def createLaboredgeDF():
    laboredge = pd.read_excel('laboredge.xlsx')
    laboredge = laboredge.drop('Unnamed: 20', 1)
    laboredge = laboredge[laboredge['Deal Date'].isnull() != True]
    laboredge['Case #'] = laboredge['Case #'].astype(str)
    laboredge = laboredge[laboredge['Case #'] != 'nan']
    laboredge['Case #'] = laboredge['Case #'].replace(['0 Non-Case'], '35980')
    laboredge['Service Provider U'] = laboredge['Service Provider'].str.upper()
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['AMAZON WEB SERVICES', 'AWS'],'AMAZON')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['IMB'],'IBM')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['SLUNK'],'SPLUNK')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['BLACKBOX SERVICES'],'BLACKBOX')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['CONVERGEONE'],'CONVERGE ONE')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['TECH MAHINDRA'],'TECH MAHINDRA LIMITED')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['PRACTICAL DATA SYSTEMS'],'PRACTICAL DATA SOLUTIONS')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['HACKETT GROUP'],'THE HACKETT GROUP')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['SHELBY GROUP'],'THE SHELBY GROUP')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['CAPGEMINI / SOGETI'],'SOGETI')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['VERTEX'],'VERTEX INC')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['AVENA'],'AVEVA')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['MU SIGMA', 'MU SIGMA INC'],'MU SIGMA INC.')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['PROJECT CORPS'],'PROJECTCORPS')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['VERITAS'],'VERITAS (SYMANTEC)')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['BDIPLUS, INC.'],'BDIPLUS')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['ASSIMILATE'],'ASSIMILATE CONSULTING')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['HPE'],'HEWLETT PACKARD ENTERPRISE (HPE)')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['ITCONVERGENCE'],'IT CONVERGENCE')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['EY'],'ERNST & YOUNG')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['NTT DATA'],'NTT')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['TECHINCAL SERVICES INC.'],'TECHNICAL SERVICES INC.')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['APEX'],'APEX SYSTEMS')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['CALIBER'],'CALIBER SECURITY PARTNERS')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['YAANA'],'YAANA TECHNOLOGIES')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['ITC'],'ITC INFOTECH')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['JDA'],'BLUE YONDER (JDA)')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['BANYAN TECHNOLOGIES'],'BANYAN TECHNOLOGY')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['ATG '],'ATG')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['ALLEGIS GLOBAL SOLUTION'],'ALLEGIS GLOBAL SOLUTIONS')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['TEK SYSTEMS'],'TEKSYSTEMS')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['BAYSIDE'],'BAYSIDE RESOURCES')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['ANDROIT'],'ADROIT RESOURCES')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['WWT'],'WWT (VAR)')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['AVI-SPL'],'AUDIO VISUAL INNOVATIONS')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['HTC GLOBAL'],'HTC GLOBAL SERVICES')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['AXIS'],'AXIS TECHNOLOGIES')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['WIREDCRAFT'],'WIREDCRAFT LIMITED')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['TOBIAS INTERNATIONAL'],'TOBIAS CONSULTING')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['SOLID TECHNOLOGIES'],'SOLID TECHNOLOGY')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['LEARNING ADMINISTRATION SERVICES'],'LEARNING ADMINISTRATION SERVICES (LAS)')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['FLUX 7'],'FLUX7')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['COLLABNET'],'COLLABNET VERSIONONE')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['MICROFOCUS'],'MICRO FOCUS')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['DELOITTE '],'DELOITTE')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['INFOR (LAWSON)'],'INFOR')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['CROSS COUNTRY'],'CROSSCOUNTRY CONSULTING')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['HORIZONS CONSULTING'],'HORIZON CONSULTING')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['TOOLSGROUP'],'TOOL GROUP BROOKES')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['SPAIENT'],'SAPIENT')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['INSIGHT GLOBAL '],'INSIGHT GLOBAL') 
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['GE'],'GENERAL ELECTRIC - GE')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['SQUARE1'],'SQUARE ONE ADVERTISING')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['ONESTREAM'],'ONESTREAM SOFTWARE')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['CHECKPOINT'],'CHECK POINT SOFTWARE TECHNOLOGIES')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['PERSISTENT'],'PERSISTENT SYSTEMS')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['ACTIVE CYBER '],'ACTIVE CYBER')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['CISCO APPDYNAMICS', 'APPDYNAMICS'],'CISCO')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['EMC'],'DELL EMC')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['AVENGERS'],'AVENGERS CONSULTING GROUP')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['SESSION M'],'SESSIONM')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['DASSAULT', 'DASSAULT SYSTEMS'],'DASSAULT SYSTEMES')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['TATA'],'TATA AMERICA INTERNATIONAL')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['COGIZANT'],'COGNIZANT')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['NUEXO'],'NUXEO')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['PWC '],'PWC')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['HURON'],'HURON CONSULTING')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['NAVIGANT'],'NAVIGANT CONSULTING')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['CLARKSTON CONSULTING'],'CLARKSTON-POTOMAC GROUP')
    laboredge['Service Provider U'] = laboredge['Service Provider U'].replace(['FUJISTU'],'FUJITSU')
    laboredge['Service Provider U'].fillna("NONE SPECIFIED", inplace = True)

    laboredge = laboredge.sort_values('Case #')

    idx = laboredge[(laboredge['Client'] == 'Deutsche Bank') & (laboredge['Service Provider'] == 'Yash Technologies')]['Case #'].index
    for i in idx:
        laboredge.loc[i,'Case #'] = '21944'
    idx = laboredge[(laboredge['Client'] == 'Boeing') & (laboredge['Service Provider'] == 'Mentor Graphics')]['Case #'].index
    for i in idx:
        laboredge.loc[i,'Case #'] = '14908'
    idx = laboredge[(laboredge['Client'] == 'SmartSheet') & (laboredge['Service Provider'] == 'Protiviti')]['Case #'].index
    for i in idx:
        laboredge.loc[i,'Case #'] = '11681'

    laboredge['OEM Company'] = laboredge['OEM Company'].replace(['Identrophy'], 'Identropy')
    laboredge['OEM Company'] = laboredge['OEM Company'].replace(['Epic Software'], 'Epic')

    idx = laboredge[laboredge['Case #'] == '17899']['OEM Company'].index
    for i in idx:
        laboredge.loc[i, 'OEM Company'] = '7Summits'
    
    idx = laboredge[laboredge['Case #'] == '31052']['OEM Company'].index
    for i in idx:
        laboredge.loc[i, 'OEM Company'] = 'IBM'

    idx = laboredge[laboredge['Case #'] == '27336']['OEM Company'].index
    for i in idx:
        laboredge.loc[i, 'OEM Company'] = None

    idx = laboredge[laboredge['Case #'] == '35731']['OEM Company'].index
    for i in idx:
        laboredge.loc[i, 'Service Provider U'] = 'EBIX'

    laboredge['Deal Total'] = laboredge['Deal Total'].replace(' ', '')
    laboredge['Deal Total'] = laboredge['Deal Total'].replace('$', '')
    laboredge['Deal Total'] = laboredge['Deal Total'].replace('$0-10M', float(0))
    laboredge['Deal Total'] = laboredge['Deal Total'].replace('$88k', float(88 * 1000))
    laboredge['Deal Total'] = laboredge['Deal Total'].replace('$290k', float(290 * 1000))
    laboredge['Deal Total'] = laboredge['Deal Total'].replace('$21k', float(21 * 1000))
    laboredge['Deal Total'] = laboredge['Deal Total'].replace('$294k', float(290 * 1000))
    laboredge['Deal Total'] = laboredge['Deal Total'].replace('$4.6M', float(4.6 * 1000000))
    laboredge['Deal Total'] = laboredge['Deal Total'].replace('$27.8M', float(27.8 * 1000000))
    laboredge['Deal Total'] = laboredge['Deal Total'].replace('$4M', float(4 * 1000000))

    return laboredge

def splitColumns(fileDF):
    splits = fileDF['case - file - extension'].split('|')
    fileDF['Case #'] = splits[0]
    fileDF['File'] = splits[1]
    return fileDF

def createFileDF():
    files = pd.read_csv('listoffiles.csv')
    files = files.drop(['Unnamed: 1', 'Unnamed: 2', 'Unnamed: 3', 'Unnamed: 4'], 1)
    files = files.apply(splitColumns, axis=1)
    return files

def joinObjX(SupplierName, CaseNum, objectxsDF):
    sameSupplier = objectxsDF[objectxsDF['Service Provider'] == SupplierName]
    sameCase = sameSupplier[sameSupplier['Case #'] == CaseNum]
    if len(sameCase.index) == 1:
        return sameCase['ID'].iloc[0]
    return None

def checkForDocName(caseNum, docName, fileDF):
    fileName = docName
    if fileName != 'nan':
        filesForCase = fileDF[fileDF['Case #'] == caseNum]
        listOfFiles = filesForCase['File'].tolist()
        alreadyFoundFile = False
        for f in listOfFiles:
            if docName in f:
                if alreadyFoundFile:
                    fileName = docName
                else:
                    fileName = f
    return fileName

def checkForValidDoc(caseNum, docName, fileDF):
    filesForCase = fileDF[fileDF['Case #'] == caseNum]
    listOfFiles = filesForCase['File'].tolist()
    if docName in listOfFiles:
        return True
    return False

def createLaboredgeMergedDF(laboredge, objxs, files):
    laboredgeMerged = laboredge.copy()
    laboredgeMerged['Object X ID'] = laboredgeMerged.apply(lambda row: joinObjX(row['Service Provider U'], row['Case #'], objxs), axis=1)
    ##laboredgeMerged['Document Name'] = laboredgeMerged.apply(lambda row: checkForDocName(row['Case #'], str(row['Document Name']), files), axis=1)
    ##laboredgeMerged['Valid Document'] = laboredgeMerged.apply(lambda row: checkForValidDoc(row['Case #'], row['Document Name'], files), axis=1)
    return laboredgeMerged

def ratesToFloats(laboredgeDF):
    hours = laboredgeDF['Hours']
    if type(hours) == str:
        laboredgeDF['Hours'] = float(hours)
        print('HOURS STRING')

    expHigh = laboredgeDF['Years Exp High']
    if type(expHigh) == str:
        laboredgeDF['Years Exp High'] = float(expHigh)
        print('EXP HIGH STRING')

    expLow = laboredgeDF['Years Exp Low']
    if type(expLow) == str:
        laboredgeDF['Years Exp Low'] = float(expLow)
        print('EXP LOW STRING')

    qtdHigh = laboredgeDF['Quoted Rate High']
    if type(qtdHigh) == str:
        laboredgeDF['Quoted Rate High'] = float(qtdHigh)
        print('QTD HIGH STRING')

    qtdLow = laboredgeDF['Quoted Rate Low']
    if type(qtdLow) == str:
        laboredgeDF['Quoted Rate Low'] = float(qtdLow)
        print('QTD LOW STRING')

    tgtHigh = laboredgeDF['CEP Provided Rate High']
    if type(tgtHigh) == str:
        laboredgeDF['CEP Provided Rate High'] = float(tgtHigh)
        print('TGT HIGH STRING')

    tgtLow = laboredgeDF['CEP Provided Rate Low']
    if type(tgtLow) == str:
        laboredgeDF['CEP Provided Rate Low'] = float(tgtLow)
        print('TGT LOW STRING')

    dealTotal = laboredgeDF['Deal Total']
    if type(dealTotal) == str:
        print(dealTotal)
        if dealTotal == '0-10M':
            dealTotal = 0
        if dealTotal in ['88k','290k', '21k', '294k']:
            dealTotal = float(dealTotal.replace('k', '')) * 1000
        if dealTotal in ['4.6M', '27.8M', '4M']:
            dealTotal = float(dealTotal.replace('M', '')) * 1000000
        laboredgeDF['Deal Total'] = float(dealTotal)
        print(laboredgeDF['Deal Total'])
    return laboredgeDF

def createLaboredgeMatchedDF(laboredgeMerged):
    laboredgeMatched = laboredgeMerged[laboredgeMerged['Object X ID'].isnull() != True]
    laboredgeMatched['Input Date'] = laboredgeMatched['Deal Date']
    laboredgeMatched['Quoted Rate High'] = laboredgeMatched['qtd High']
    laboredgeMatched['Quoted Rate Low'] = laboredgeMatched['qtd Low']
    laboredgeMatched['CEP Provided Rate High'] = laboredgeMatched['tgt High']
    laboredgeMatched['CEP Provided Rate Low'] = laboredgeMatched['tgt Low']
    laboredgeMatched['Years Exp High'] = laboredgeMatched['exp High']
    laboredgeMatched['Years Exp Low'] = laboredgeMatched['exp Low']
    ##laboredgeMatched = laboredgeMatched.drop(['Deal Date', 'Service Provider', 'Client', 'Service Provider U', 'qtd High', 'exp Low', 'tgt Low', 'qtd Low', 'exp High', 'tgt High', 'Case #', 'Valid Document'], 1)
    laboredgeMatched = laboredgeMatched.drop(['Deal Date', 'Service Provider', 'Client', 'Service Provider U', 'qtd High', 'exp Low', 'tgt Low', 'qtd Low', 'exp High', 'tgt High', 'Case #'], 1)
    cols = ['Input Date', 'Object X ID', 'OEM Company', 'Product/Category', 'Level', 'Position Title', 'Hours', 'Years Exp High', 'Years Exp Low', 'Quoted Rate High', 'Quoted Rate Low', 'CEP Provided Rate High', 'CEP Provided Rate Low', 'Deal Total', 'Country', 'City/State', 'Currency Notes', 'Document Name']
    laboredgeMatched = laboredgeMatched[cols]
    laboredgeMatched = laboredgeMatched.apply(ratesToFloats, axis=1)
    laboredgeMatched['Database Practice'] = 'Services'
    return laboredgeMatched