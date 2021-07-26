import pandas as pd
import sys #imports sys because we are using it at command line for input and output
import op #imports operations file op.py

#Defining begin function which will inturn return transaction table as an output

def begin(TransactionId, timeStamp, TransactionTable):
    TransactionTable = TransactionTable.append({'TransactionID': TransactionId, 'TimeStamp': timeStamp, 'State': 'Active',\
                                            'ItemsLocked': [], 'WaitingOn': None}, ignore_index=True)
    return TransactionTable


def read(TransactionId,dataItem,TransactionTable,LogTable,Flag, fileObj):
	#check if transaction id already exits in transacrion table
    if any(TransactionTable.TransactionID == TransactionId):
		#get the row no. from the transaction table of the given transaction id
        row_index = TransactionTable[TransactionTable.TransactionID == TransactionId].index.values.astype(int)[0]
		#get the timestamp of the transaction
        timeStampOfRequestingTr = TransactionTable.at[row_index, 'TimeStamp']
        #check if transaction is Aborted, if Aborted ignore the operation and return success so that it will be deleted
        if TransactionTable.at[row_index, 'State'] == 'Aborted':
            printData = op.Line
            printData += '\nIgnore Aborted Transaction ' + str(TransactionTable.at[row_index, 'TransactionID'])
            printToFile(printData, fileObj)
            return op.ABORTED, TransactionTable, LogTable
		#check if transaction is Blocked, if so ignore the the operation
		#Flag = false represents transaction cannot be unBlocked
        if TransactionTable.at[row_index, 'State'] == 'Blocked' and Flag!=True:
            printData = op.Line
            printData += '\nIgnore Blocked Transaction ' + str(TransactionTable.at[row_index, 'TransactionID'])
            printToFile(printData, fileObj)
            return op.IGNORE,TransactionTable,LogTable
        #check if requested data item already present in lock table
        elif any(LogTable.Items == dataItem):
			#get the row no. from the lock table where that data item exists
            rowLckIndex = LogTable[LogTable.Items == dataItem].index.values.astype(int)[0]
			#check if already wl lock is on that data item
            if LogTable.at[rowLckIndex, 'LockState'] == 'wl':
				#get the time stamp of transaction which is holding the wl lock
                holdingTransactionIDList = LogTable.at[rowLckIndex, 'LockingTransaction']
                rowIndex = TransactionTable[TransactionTable.TransactionID == holdingTransactionIDList[0]].index.values.astype(int)[0]
                timeStampOfHoldingTr = TransactionTable.at[rowIndex, 'TimeStamp']
				#check if requesting transaction is younger
                if timeStampOfRequestingTr > timeStampOfHoldingTr:
					#lock requesting transaction is younger; abort the transaction
                    TransactionTable, LogTable = abort(TransactionId, TransactionTable, LogTable, fileObj)
                    printData = op.Line
                    printData += '\nAbort Transaction ' + str(TransactionTable.at[row_index, 'TransactionID'])
                    printToFile(printData, fileObj)
                    return op.ABORT,TransactionTable,LogTable
                else:
					#lock requesting transaction is older; block the transaction
                    TransactionTable, LogTable = wait(TransactionId, dataItem, TransactionTable, LogTable)
                    printData = op.Line
                    printData += '\nBlock Transaction ' + str(TransactionTable.at[row_index, 'TransactionID'])
                    printToFile(printData, fileObj)
                    return op.IGNORE,TransactionTable,LogTable
            else:
				#No locks on data item, grant lock to requesting transaction
                LogTable.at[rowLckIndex, 'LockingTransaction'].append(TransactionId)
                TransactionTable.at[row_index,'ItemsLocked'].append([dataItem,'rl'])
                return op.SUCCESS,TransactionTable,LogTable
        else:
			#data item is not present in lock table, hence no locks on data item, grant lock to requesting transaction
			#In case requesting transaction is Blocked and Flag is True, unblock it and grant access to data item.
            if TransactionTable.at[row_index, 'State'] == 'Blocked' and Flag:
                printData = op.Line
                printData += '\nActive again Transaction ' + str(TransactionTable.at[row_index, 'TransactionID'])
                printToFile(printData, fileObj)
                TransactionTable.at[row_index, 'State'] = 'Active'
                TransactionTable.at[row_index, 'WaitingOn'] = None
			#Add entries to transaction table and lock table
            LogTable = LogTable.append({'Items':dataItem,'LockState':"rl",'LockingTransaction':[TransactionId],\
                                         'TransactionsWaiting':[]},ignore_index=True)
            TransactionTable.at[row_index, 'ItemsLocked'].append([dataItem, 'rl'])
            return op.SUCCESS,TransactionTable,LogTable
    return op.IGNORE, TransactionTable, LogTable

def write(TransactionId, dataItem, Flag, TransactionTable, LogTable, fileObj):
	#check if transaction id already exits in transacrion table
    if any(TransactionTable.TransactionID == TransactionId):
		#get the row no. from the transaction table of the given transaction id
        row_index = TransactionTable[TransactionTable.TransactionID == TransactionId].index.values.astype(int)[0]
		#get the timestamp of the transaction
        timeStampOfRequestingTr = TransactionTable.at[row_index, 'TimeStamp']
        #check if transaction is Aborted, if Aborted ignore the operation and return success so that it will be deleted
        if TransactionTable.at[row_index, 'State'] == 'Aborted':
            printData = op.Line
            printData += '\nIgnore Aborted Transaction ' + str(TransactionTable.at[row_index, 'TransactionID'])
            printToFile(printData, fileObj)
            return op.ABORTED, TransactionTable, LogTable
        #check if transaction is Blocked, if so ignore the the operation
		#Flag = false represents transaction cannot be unBlocked
        if TransactionTable.at[row_index, 'State'] == 'Blocked' and Flag!=True:
            printData = op.Line
            printData += '\nIgnore Blocked Transaction ' + str(TransactionTable.at[row_index, 'TransactionID'])
            printToFile(printData, fileObj)
            return op.IGNORE, TransactionTable, LogTable
        #check if requested data item already present in lock table
        elif any(LogTable.Items == dataItem):
			#get the row no. from the lock table where that data item exists
            rowLckIndex = LogTable[LogTable.Items == dataItem].index.values.astype(int)[0]
			#check if already rl or wl lock is on that data item
            if LogTable.at[rowLckIndex, 'LockState'] == 'rl' or \
                LogTable.at[rowLckIndex, 'LockState'] == 'wl':
                TransactionList = LogTable.at[rowLckIndex, 'LockingTransaction']
				#check if only 1 transaction has the lock
                if len(TransactionList) == 1:
					#check if same transaction which has rl lock try to get wl on data item
                    if TransactionList[0] == TransactionId:
                        #upgrade
                        if TransactionTable.at[row_index, 'State'] == 'Blocked' and Flag:
                            printData = op.Line
                            printData += '\nActive again Transaction ' + str(TransactionTable.at[row_index, 'TransactionID'])
                            printToFile(printData, fileObj)
                            TransactionTable.at[row_index, 'State'] = 'Active'
                            TransactionTable.at[row_index, 'WaitingOn'] = None

                            #delete transaction id from lock table column "transaction Waiting"
                            delTrIndex = LogTable.at[rowLckIndex, 'TransactionsWaiting'].index(TransactionId)
                            trWaitingList = LogTable.at[rowLckIndex, 'TransactionsWaiting']
                            del trWaitingList[delTrIndex]
                            LogTable.at[rowLckIndex, 'TransactionsWaiting'] = trWaitingList

						#add one more data item with 'wl' to transaction table under items locked column
                        LogTable.at[rowLckIndex, 'LockState'] = 'wl'
                        tr = TransactionTable.at[row_index, 'ItemsLocked']
                        for i in range(len(tr)):
                            if tr[i] == [dataItem,"rl"]:
                                tr[i] = [dataItem,"wl"]
                        TransactionTable.at[row_index, 'ItemsLocked'] = tr
                        return op.SUCCESS, TransactionTable, LogTable
				#Comes here if already rl or wl lock is present by another transaction/s
                for transaction in TransactionList:
					#check time stamp of every transaction with requesting transaction's time stamp
                    rowIndex = TransactionTable[TransactionTable.TransactionID == transaction].index.values.astype(int)[0]
                    if timeStampOfRequestingTr > TransactionTable.at[rowIndex, 'TimeStamp']:
						#requesting transaction is younger; abort the transaction
                        TransactionTable, LogTable = abort(TransactionId, TransactionTable, LogTable, fileObj)
                        printData = op.Line
                        printData += '\nAbort Transaction ' + str(TransactionTable.at[row_index, 'TransactionID'])
                        printToFile(printData, fileObj)
                        return op.ABORT, TransactionTable, LogTable
				#requesting transaction is older; block the requesting transaction
                TransactionTable, LogTable = wait(TransactionId, dataItem, TransactionTable, LogTable)
                printData = op.Line
                printData += '\nBlock Transaction ' + str(TransactionTable.at[row_index, 'TransactionID'])
                printToFile(printData, fileObj)
                return op.IGNORE, TransactionTable, LogTable
        else:
			#Comes here if data item doesn't exists in lock table, hence lock can be granted
			#In case transaction is Blocked/waiting on data item and Flag is True, activate transaction and grant the lock
            if TransactionTable.at[row_index, 'State'] == 'Blocked' and Flag:
                printData = op.Line
                printData += '\nActive again Transaction ' + str(TransactionTable.at[row_index, 'TransactionID'])
                printToFile(printData, fileObj)
                TransactionTable.at[row_index, 'State'] = 'Active'
                TransactionTable.at[row_index, 'WaitingOn'] = None
			#add entries to transaction table and lock table
            LogTable = LogTable.append({'Items': dataItem, 'LockState': 'wl', 'LockingTransaction': [TransactionId],
                                'TransactionsWaiting': []}, ignore_index=True)
            TransactionTable.at[row_index, 'ItemsLocked'].append([dataItem,"wl"])
            return op.SUCCESS, TransactionTable, LogTable
    return op.IGNORE, TransactionTable, LogTable

def wait(TransactionId, dataItem, TransactionTable, LogTable):
	#get row no. from transaction table of given transaction id.
    row_index = TransactionTable[TransactionTable.TransactionID == TransactionId].index.values.astype(int)[0]
	#check if transaction is already Blocked
    if TransactionTable.at[row_index, 'State'] == 'Blocked':
        return TransactionTable, LogTable
	#Comes here if transaction wasn't Blocked, hence block the transaction
	#update transaction table and lock table
    TransactionTable.at[row_index, 'State'] = 'Blocked'
    TransactionTable.at[row_index, 'WaitingOn'] = dataItem

    rowLckIndex = LogTable[LogTable.Items == dataItem].index.values.astype(int)[0]
    LogTable.at[rowLckIndex, 'TransactionsWaiting'].append(TransactionId)
    return TransactionTable, LogTable


#defining abort function
def abort(TransactionId, TransactionTable, LogTable, fileObj):
    count_drop = 0
    row_index = TransactionTable[TransactionTable.TransactionID == TransactionId].index.values.astype(int)[0]
    #If transaction is Blocked/Aborted, ignore operation.
    if TransactionTable.at[row_index, 'State'] == 'Aborted' or \
        TransactionTable.at[row_index, 'State'] == 'Blocked':
            printData = op.Line
            printData += '\nIgnore Aborted Transaction ' + str(TransactionTable.at[row_index, 'TransactionID'])
            printToFile(printData, fileObj)
            return TransactionTable, LogTable
    #Comes here if transaction is Active.
	#Abort the transaction and update accordingly in transaction table
	#Unlock all locked data items by this transaction
    TransactionTable.at[row_index, 'State'] = 'Aborted'
    TransactionTable.at[row_index, 'ItemsLocked'] = []
    TransactionTable.at[row_index, 'WaitingOn'] = None
    for index, dILockingTransaction in enumerate(LogTable['LockingTransaction']):
        if TransactionId in dILockingTransaction:
            trIndex = dILockingTransaction.index(TransactionId)
            del dILockingTransaction[trIndex]
            if not dILockingTransaction:
                LogTable = LogTable.drop(LogTable.index[[index - count_drop]])
                count_drop+=1

    return TransactionTable, LogTable

def commit(TransactionId, TransactionTable, LogTable, fileObj):
    count_drop = 0
    row_index = TransactionTable[TransactionTable.TransactionID == TransactionId].index.values.astype(int)[0]
    #check if transaction is Aborted, if so then ignore operation and return success so that operation will be removed
    if TransactionTable.at[row_index, 'State'] == 'Aborted':
        printData = op.Line
        printData += '\nIgnore Aborted Transaction ' + str(TransactionTable.at[row_index, 'TransactionID'])
        printToFile(printData, fileObj)
        return op.ABORTED, TransactionTable, LogTable
    #check if transaction Blocked, if so ignore the operation
    if TransactionTable.at[row_index, 'State'] == 'Blocked':
        printData = op.Line
        printData += '\nIgnore Blocked Transaction ' + str(TransactionTable.at[row_index, 'TransactionID'])
        printToFile(printData, fileObj)
        return op.IGNORE, TransactionTable, LogTable
	#Comes here if transaction is Active.
	#Commit the transaction and update accordingly in transaction table
	#Unlock all locked data items by this transaction
    TransactionTable.at[row_index, 'State'] = 'committed'
    TransactionTable.at[row_index, 'ItemsLocked'] = []
    TransactionTable.at[row_index, 'WaitingOn'] = None
    for index, dILockingTransaction in enumerate(LogTable['LockingTransaction']):
        if TransactionId in dILockingTransaction:
            trIndex = dILockingTransaction.index(TransactionId)
            del dILockingTransaction[trIndex]
            if not dILockingTransaction:
                LogTable = LogTable.drop(LogTable.index[[index - count_drop]])
                count_drop+=1

    return op.SUCCESS, TransactionTable, LogTable


def openOutputFile(fileName):
    try:
        fObj = open(fileName,'a+')
    except:
        sys.exit("\nError opening output file.\nCheck file name.\n")
    return fObj

def closeOutputFile(fileObj):
    fileObj.close()

def printToFile(data, fileObj):
    fileObj.write(data)

def main(argv):
    #Check input and output file names are provided through command line
    if len(argv) != 3:
        print('Two command-line arguments are needed:')
        print('Usage: %s [input_file_Name] [output_file_name]' % argv[0])
        sys.exit(2)
	#get input and output file names
    inputFileName = argv[op.OFFS_INPUT_FILE_NAME]
    outputFileName = argv[op.OFFS_OUTPUT_FILE_NAME]

	#Declare Transaction table and Lock table
    Flag = False
    timeStamp = 0
    TransactionTable = pd.DataFrame(columns=['Transactionid', 'TimeStamp', 'State', 'ItemsLocked', \
                                           'WaitingOn'])
    TransactionTable = TransactionTable.astype('object')
    LogTable = pd.DataFrame(columns=['Items','LockState','LockingTransaction', \
                                      'TransactionsWaiting'])
    LogTable = LogTable.astype('object')

    # Try to open the input file
    try:
        fObj = open(inputFileName,'r+')
    except:
        sys.exit("\nError opening input file.\nCheck file name.\n")
    fileLines= list(map(str.strip, fObj.read().split(';')))
    fObj.close()

    # Try to open the output file
    fileObj = openOutputFile(outputFileName)

    LinesIndex = 0
	#Read first operation
    line = fileLines[LinesIndex]
    #for line in lines:
    while len(line):
        printData = op.Line
        printData += op.Line + '\n' + str(line)
        printData += '\n'
		#write operation to file
        printToFile(printData, fileObj)

        if line[op.OFFS_OPERATION] == op.BEGIN:
			#Operation is bi (Begin i)
            TransactionId = line[op.T_ID]
            timeStamp += 1
            TransactionTable = begin(TransactionId, timeStamp, TransactionTable)
            Flag = False
            del fileLines[LinesIndex]
            printData = "\nBegin T"+TransactionId+'\n\n'+str(TransactionTable) + '\n' + str(LogTable) + op.Line
            printToFile(printData, fileObj)

        elif line[op.OFFS_OPERATION] == op.READ:
            #Operation is ri(data_item) (read operation by i on data item)
			#retrive data item and transaction id i
            TransactionId = line[op.T_ID]
            dataItem = line[line.index('(')+1]
            success, TransactionTable, LogTable = read(TransactionId,dataItem,TransactionTable,LogTable,\
                                                      Flag, fileObj)
            if success == op.ABORT:
				#Come here if because of the read operation transaction got Aborted
                del fileLines[LinesIndex]
                printData = "\nIgnore as T"+ TransactionId +" was Aborted"
				#reinitialize the pointer to operation in file to 0.(Try to execute previous commands that coudn't be executed)
                LinesIndex = 0
                Flag = True
            elif success == op.SUCCESS:
				#Come here if read operation(read lock) by transaction was successful
                Flag = False
                del fileLines[LinesIndex]
                printData = "\nRead lock acquired by T"+TransactionId+ " on "+dataItem
            elif success == op.IGNORE:
				#Come here if operation was ignored because transaction was Blocked(waiting)
                Flag = False
                printData = "\nIgnore T"+ TransactionId
				#Increment and get next operation
                LinesIndex += 1
            elif success == op.ABORTED:
                #Come here if transaction was Aborted
                Flag = False
                del fileLines[LinesIndex]
                printData = "\nT"+TransactionId+ " was Aborted"
            #write resulting tables to file
            printData += '\n\n'
            printData += str(TransactionTable) + '\n' + str(LogTable) + op.Line
            printToFile(printData, fileObj)

        elif line[op.OFFS_OPERATION] == op.WRITE:
			#Operation is wi(data_item) (write operation by i on data item)
			#retrive data item and transaction id i
            TransactionId = line[op.T_ID]
            dataItem = line[line.index('(')+1]
            success, TransactionTable, LogTable = write(TransactionId, dataItem, Flag, \
                                                       TransactionTable, LogTable, fileObj)
            if success == op.ABORT:
				#Come here if because of the write operation transaction got Aborted
                del fileLines[LinesIndex]
                Flag = True
                printData = "\nIgnore as T"+ TransactionId +"was Aborted"
				#reinitialize the pointer to operation in file to 0.(Try to execute previous commands that coudn't be executed)
                LinesIndex = 0
                #print(fileLines)
            elif success == op.SUCCESS:
				#Come here if read operation(read lock) by transaction was successful
                Flag = False
                del fileLines[LinesIndex]
                printData = "\nWrite lock acquired by T"+TransactionId+ " on "+dataItem
            elif success == op.IGNORE:
                #Come here if operation was ignored because transaction was Blocked(waiting)
                Flag = False
                printData = "\nIgnore T"+ TransactionId
				#Increment and get next operation
                LinesIndex += 1
            elif success == op.ABORTED:
                #Come here if transaction was Aborted


                Flag = False
                del fileLines[LinesIndex]
                printData = "\nT"+TransactionId+ " was Aborted"
			#write resulting tables to file
            printData += '\n\n'
            printData += str(TransactionTable) + '\n' + str(LogTable) + op.Line
            printToFile(printData, fileObj)

        elif line[op.OFFS_OPERATION] == op.END:
            #Operation is ei (end (commit) operation of transaction i)
			#retrive transaction id i
            TransactionId = line[op.T_ID]
            success, TransactionTable, LogTable = commit(TransactionId, TransactionTable, LogTable, fileObj)
            if success == op.SUCCESS:
				#Come here if successfully commited
                del fileLines[LinesIndex]
                Flag = True
                printData = "\nT"+TransactionId+" Committed"
				#reinitialize the pointer to operation in file to 0.(Try to execute previous commands that coudn't be executed)
                LinesIndex = 0
            elif success == op.IGNORE:
				#Come here if transaction is Blocked(waiting) already
				#increment operation pointer
                LinesIndex += 1
                Flag = False
                printData = "\nIgnore T"+ TransactionId
            elif success == op.ABORTED:
                #Come here if transaction was Aborted
                Flag = False
                del fileLines[LinesIndex]
                printData = "\nT"+TransactionId+ " was Aborted"
			#write resulting tables to file
            printData += '\n\n'
            printData += str(TransactionTable) + '\n' + str(LogTable) + op.Line
            printToFile(printData, fileObj)
		#get new operation, LinesIndex is modified pointer to operations
        line = fileLines[LinesIndex]
    #Write final result to the file
    printData = op.Line+ '\n'
    printData += op.Line + '\n' + str(TransactionTable) + op.Line + '\n' + str(LogTable) + op.Line + '\n'
    printToFile(printData, fileObj)
    closeOutputFile(fileObj)
    return

if __name__ == '__main__':
    main(sys.argv)
