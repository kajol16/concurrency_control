import pandas as pd
import sys #imports sys because we are using it at command line for input and output
import op #imports operations file op.py

#Defining begin function which will inturn return transaction table as an output whenever it encounter begin statement it will begin a transaction

def begin(TransactionID, timeStamp, TransactionTable):
    TransactionTable = TransactionTable.append({'TrID': TransactionID, 'TimeStamp': timeStamp, 'State': 'Active',\
                                            'ItemsLocked': [], 'WaitingOn': None}, ignore_index=True)
    return TransactionTable

#Read operation will check for locks and entry in transaction table and lock table
#initially when encounters a read operation from the input file if there are no locks it
#create an entry in the lock table with read lock and transaction ID, this will also intially check if the transaction is ABORTED
#if the transaction is aborted it will ignore the operation. In the late part of this operation we check for the age of
#the transcation and we abort the transaction is the requesting transaction is younger than the holding transaction

def read(TransactionID,d_item,TransactionTable,LogTable,Flag_R, fileObject):
    if any(TransactionTable.TrID == TransactionID):
        transaction_row_index = TransactionTable[TransactionTable.TrID == TransactionID].index.values.astype(int)[0]
		#fetch the timestamp of the tr of the requesting transaction
        tSRequestingTr = TransactionTable.at[transaction_row_index, 'TimeStamp']
        if TransactionTable.at[transaction_row_index, 'State'] == 'Aborted':
            printData = op.DLine
            printData += '\nIgnore Aborted Transaction ' + str(TransactionTable.at[transaction_row_index, 'TrID'])
            printToFile(printData, fileObject)
            return op.ABORTED, TransactionTable, LogTable
            #here we check if the transaction is blocked and if it is unblockable
        if TransactionTable.at[transaction_row_index, 'State'] == 'Blocked' and Flag_R!=True:
            printData = op.DLine
            printData += '\nIgnore Blocked Transaction ' + str(TransactionTable.at[transaction_row_index, 'TrID'])
            printToFile(printData, fileObject)
            return op.IGNORE,TransactionTable,LogTable

        elif any(LogTable.Items == d_item):

            lock_row_index = LogTable[LogTable.Items == d_item].index.values.astype(int)[0]

            if LogTable.at[lock_row_index, 'LockState'] == 'wl':
				#fetch the timstamp of writelock transaction
                holdingTransactionIDList = LogTable.at[lock_row_index, 'LockingTransaction']
                rowIndex = TransactionTable[TransactionTable.TrID == holdingTransactionIDList[0]].index.values.astype(int)[0]
                tSHoldingTr = TransactionTable.at[rowIndex, 'TimeStamp']
				#if transaction requesting to lock the data item is younger than holding transaction abort the requesting transaction
                if tSRequestingTr > tSHoldingTr:

                    TransactionTable, LogTable = abort(TransactionID, TransactionTable, LogTable, fileObject)
                    printData = op.DLine
                    printData += '\nAbort Transaction ' + str(TransactionTable.at[transaction_row_index, 'TrID'])
                    printToFile(printData, fileObject)
                    return op.ABORT,TransactionTable,LogTable
                #if transaction requesting to lock the data item is older than holding transaction block the requesting transaction
                else:

                    TransactionTable, LogTable = wait(TransactionID, d_item, TransactionTable, LogTable)
                    printData = op.DLine
                    printData += '\nBlock Transaction ' + str(TransactionTable.at[transaction_row_index, 'TrID'])
                    printToFile(printData, fileObject)
                    return op.IGNORE,TransactionTable,LogTable
            else:
				#if there are no existing entries in the lock table for the transaction create an entry in log table
                LogTable.at[lock_row_index, 'LockingTransaction'].append(TransactionID)
                TransactionTable.at[transaction_row_index,'ItemsLocked'].append([d_item,'rl'])
                return op.SUCCESS,TransactionTable,LogTable
        else:
			#If the requesting data state is blocked and relock flag is true we can release the lock and grant the lock to the
            #requesting transaction

            if TransactionTable.at[transaction_row_index, 'State'] == 'Blocked' and Flag_R:
                printData = op.DLine
                printData += '\nActive again Transaction ' + str(TransactionTable.at[transaction_row_index, 'TrID'])
                printToFile(printData, fileObject)
                TransactionTable.at[transaction_row_index, 'State'] = 'Active'
                TransactionTable.at[transaction_row_index, 'WaitingOn'] = None
			#We can check and add the entries then to log table and transaction table
            LogTable = LogTable.append({'Items':d_item,'LockState':"rl",'LockingTransaction':[TransactionID],\
                                         'TransactionsWaiting':[],'TrID':[TransactionID]},ignore_index=True)
            TransactionTable.at[transaction_row_index, 'ItemsLocked'].append([d_item, 'rl'])
            return op.SUCCESS,TransactionTable,LogTable
    return op.IGNORE, TransactionTable, LogTable

#Write operation will check for locks and entry in transaction table and lock table
#initially when encounters a write operation from the input file if there are no locks it
#create an entry in the lock table with write lock and transaction ID, this will also intially check if the transaction is ABORTED
#if the transaction is aborted it will ignore the operation. In the late part of this operation we check for the age of
#the transcation and we abort the transaction is the requesting transaction is younger than the holding transaction
def write(TransactionID, d_item, Flag_R, TransactionTable, LogTable, fileObject):
	#exisitance of transaction in transaction table
    if any(TransactionTable.TrID == TransactionID):
		#get information from the transaction table, like row number with the help of transaction ID
        transaction_row_index = TransactionTable[TransactionTable.TrID == TransactionID].index.values.astype(int)[0]
		#get the timestamp of the transaction
        tSRequestingTr = TransactionTable.at[transaction_row_index, 'TimeStamp']
        #check if transaction is Aborted, if Aborted ignore the operation and return success so that it will be deleted
        if TransactionTable.at[transaction_row_index, 'State'] == 'Aborted':
            printData = op.DLine
            printData += '\nIgnore Aborted Transaction ' + str(TransactionTable.at[transaction_row_index, 'TrID'])
            printToFile(printData, fileObject)
            return op.ABORTED, TransactionTable, LogTable
        #Check if the transaction state is blocked and recheck flag is false, if so ignore the operation as it could not be performed
        #because the transaction is unblockable.
        if TransactionTable.at[transaction_row_index, 'State'] == 'Blocked' and Flag_R!=True:
            printData = op.DLine
            printData += '\nIgnore Blocked Transaction ' + str(TransactionTable.at[transaction_row_index, 'TrID'])
            printToFile(printData, fileObject)
            return op.IGNORE, TransactionTable, LogTable
        #Check for the type of lock in the log table if the lock exists(read lock or write lock)
        elif any(LogTable.Items == d_item):
            lock_row_index = LogTable[LogTable.Items == d_item].index.values.astype(int)[0]
            if LogTable.at[lock_row_index, 'LockState'] == 'rl' or \
                LogTable.at[lock_row_index, 'LockState'] == 'wl':
                TransactionList = LogTable.at[lock_row_index, 'LockingTransaction']
				#Check If there is only one transaction that has the lock and
                #if check if same transaction which has readlock is trying to get writelock on data item
                if len(TransactionList) == 1:
                    if TransactionList[0] == TransactionID:
                        #upgrade from blocked state to active state
                        if TransactionTable.at[transaction_row_index, 'State'] == 'Blocked' and Flag_R:
                            printData = op.DLine
                            printData += '\nActive again Transaction ' + str(TransactionTable.at[transaction_row_index, 'TrID'])
                            printToFile(printData, fileObject)
                            TransactionTable.at[transaction_row_index, 'State'] = 'Active'
                            TransactionTable.at[transaction_row_index, 'WaitingOn'] = None

                            #delete transaction id from lock table column "transaction Waiting"
                            delTrIndex = LogTable.at[lock_row_index, 'TransactionsWaiting'].index(TransactionID)
                            trWaitingList = LogTable.at[lock_row_index, 'TransactionsWaiting']
                            del trWaitingList[delTrIndex]
                            LogTable.at[lock_row_index, 'TransactionsWaiting'] = trWaitingList

						#add an addition write lock in locked items column in the transaction table
                        LogTable.at[lock_row_index, 'LockState'] = 'wl'
                        tr = TransactionTable.at[transaction_row_index, 'ItemsLocked']
                        for i in range(len(tr)):
                            if tr[i] == [d_item,"rl"]:
                                tr[i] = [d_item,"wl"]
                        TransactionTable.at[transaction_row_index, 'ItemsLocked'] = tr
                        return op.SUCCESS, TransactionTable, LogTable
				#if there are readlock and write lock already present this block will be executed
                for transaction in TransactionList:
					#if transaction requesting to lock the data item is younger than holding transaction abort the requesting transaction
                    rowIndex = TransactionTable[TransactionTable.TrID == transaction].index.values.astype(int)[0]
                    if tSRequestingTr > TransactionTable.at[rowIndex, 'TimeStamp']:
                        TransactionTable, LogTable = abort(TransactionID, TransactionTable, LogTable, fileObject)
                        printData = op.DLine
                        printData += '\nAbort Transaction ' + str(TransactionTable.at[transaction_row_index, 'TrID'])
                        printToFile(printData, fileObject)
                        return op.ABORT, TransactionTable, LogTable
				#if transaction requesting to lock the data item is older than holding transaction block the requesting transaction
                TransactionTable, LogTable = wait(TransactionID, d_item, TransactionTable, LogTable)
                printData = op.DLine
                printData += '\nBlock Transaction ' + str(TransactionTable.at[transaction_row_index, 'TrID'])
                printToFile(printData, fileObject)
                return op.IGNORE, TransactionTable, LogTable
        else:
			#if there are no existing entries in the lock table for the transaction create an entry in log table if state is blocked,
            #activate it and grant a lock
            if TransactionTable.at[transaction_row_index, 'State'] == 'Blocked' and Flag_R:
                printData = op.DLine
                printData += '\nActive again Transaction ' + str(TransactionTable.at[transaction_row_index, 'TrID'])
                printToFile(printData, fileObject)
                TransactionTable.at[transaction_row_index, 'State'] = 'Active'
                TransactionTable.at[transaction_row_index, 'WaitingOn'] = None

            LogTable = LogTable.append({'Items': d_item, 'LockState': 'wl', 'LockingTransaction': [TransactionID],
                                'TransactionsWaiting': [],'TrID':[TransactionID]}, ignore_index=True)
            TransactionTable.at[transaction_row_index, 'ItemsLocked'].append([d_item,"wl"])
            return op.SUCCESS, TransactionTable, LogTable
    return op.IGNORE, TransactionTable, LogTable

#Wait operation will check if there is a transaction and if it is already in blocked state, if not it will change it to
#blocked state and will update the transaction and the lock table accordingly
def wait(TransactionID, d_item, TransactionTable, LogTable):
    transaction_row_index = TransactionTable[TransactionTable.TrID == TransactionID].index.values.astype(int)[0]
    if TransactionTable.at[transaction_row_index, 'State'] == 'Blocked':
        return TransactionTable, LogTable
    TransactionTable.at[transaction_row_index, 'State'] = 'Blocked'
    TransactionTable.at[transaction_row_index, 'WaitingOn'] = d_item

    lock_row_index = LogTable[LogTable.Items == d_item].index.values.astype(int)[0]
    LogTable.at[lock_row_index, 'TransactionsWaiting'].append(TransactionID)
    return TransactionTable, LogTable


#defining abort function, if the transaction is already blocked or aborted the operation will get ignored
#else if it is active it will change to state to aborted.
def abort(TransactionID, TransactionTable, LogTable, fileObject):
    count_drop = 0
    transaction_row_index = TransactionTable[TransactionTable.TrID == TransactionID].index.values.astype(int)[0]
    if TransactionTable.at[transaction_row_index, 'State'] == 'Aborted' or \
        TransactionTable.at[transaction_row_index, 'State'] == 'Blocked':
            printData = op.DLine
            printData += '\nIgnore Aborted Transaction ' + str(TransactionTable.at[transaction_row_index, 'TrID'])
            printToFile(printData, fileObject)
            return TransactionTable, LogTable

    TransactionTable.at[transaction_row_index, 'State'] = 'Aborted'
    TransactionTable.at[transaction_row_index, 'ItemsLocked'] = []
    TransactionTable.at[transaction_row_index, 'WaitingOn'] = None
    for index, dILockingTransaction in enumerate(LogTable['LockingTransaction']):
        if TransactionID in dILockingTransaction:
            trIndex = dILockingTransaction.index(TransactionID)
            del dILockingTransaction[trIndex]
            if not dILockingTransaction:
                LogTable = LogTable.drop(LogTable.index[[index - count_drop]])
                count_drop+=1

    return TransactionTable, LogTable
#Commit operation will intitally check if the transaction is aborted,
#if so the same will be removed from the the tables
#if no it will check if the transaction is blocked, in that case it will end up ignoring the operations
#if the transaction is active it will commit the transaction in the transaction table accordingly
#it will then in turn unlock all the data items which were locked by this transaction.
def commit(TransactionID, TransactionTable, LogTable, fileObject):
    count_drop = 0
    transaction_row_index = TransactionTable[TransactionTable.TrID == TransactionID].index.values.astype(int)[0]
    if TransactionTable.at[transaction_row_index, 'State'] == 'Aborted':
        printData = op.DLine
        printData += '\nIgnore Aborted Transaction ' + str(TransactionTable.at[transaction_row_index, 'TrID'])
        printToFile(printData, fileObject)
        return op.ABORTED, TransactionTable, LogTable
    if TransactionTable.at[transaction_row_index, 'State'] == 'Blocked':
        printData = op.DLine
        printData += '\nIgnore Blocked Transaction ' + str(TransactionTable.at[transaction_row_index, 'TrID'])
        printToFile(printData, fileObject)
        return op.IGNORE, TransactionTable, LogTable

    TransactionTable.at[transaction_row_index, 'State'] = 'committed'
    TransactionTable.at[transaction_row_index, 'ItemsLocked'] = []
    TransactionTable.at[transaction_row_index, 'WaitingOn'] = None
    for index, dILockingTransaction in enumerate(LogTable['LockingTransaction']):
        if TransactionID in dILockingTransaction:
            trIndex = dILockingTransaction.index(TransactionID)
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

def closeOutputFile(fileObject):
    fileObject.close()

def printToFile(data, fileObject):
    fileObject.write(data)

def main(argv):
    #we have to check for input file name and output file name if it provided from the command line input and get the values of
    #inout file name and output file name.
    if len(argv) != 3:
        print('Two command-line arguments(Input File Name and Output Filename) are needed:')
        print('Usage: %s [input_file_Name] [output_file_name]' % argv[0])
        sys.exit(2)

    inputFileName = argv[op.OFF_IP_F_NAME]
    outputFileName = argv[op.OFF_OP_F_NAME]

	#Creating transaction table and the lock table declaration
    Flag_R = False
    timeStamp = 0
    TransactionTable = pd.DataFrame(columns=['TrID', 'TimeStamp', 'State', 'ItemsLocked', \
                                           'WaitingOn'])
    TransactionTable = TransactionTable.astype('object')
    LogTable = pd.DataFrame(columns=['Items','LockState','LockingTransaction', \
                                      'TransactionsWaiting','TrID'])
    LogTable = LogTable.astype('object')

    #Open the input file
    try:
        fObj = open(inputFileName,'r+')
    except:
        sys.exit("\nThere were issues opening the provid.\nCheck file name.\n")
    fileLines= list(map(str.strip, fObj.read().split(';')))
    fObj.close()

    # Try to open the output file
    fileObject = openOutputFile(outputFileName)

    LinesIndex = 0
	#Read first operation
    line = fileLines[LinesIndex]
    #for line in lines:
    while len(line):
        printData = op.DLine
        printData += op.DLine + '\n' + str(line)
        printData += '\n'
		#write operation to file
        printToFile(printData, fileObject)

        if line[op.OFFS_OPERATION] == op.BEGIN:
			#Operation is bi (Begin i)
            TransactionID = line[op.O_T_ID]
            timeStamp += 1
            TransactionTable = begin(TransactionID, timeStamp, TransactionTable)
            Flag_R = False
            del fileLines[LinesIndex]
            printData = "\nBegin T"+TransactionID+'\n\n'+str(TransactionTable) + '\n' + str(LogTable) + op.DLine
            printToFile(printData, fileObject)

        elif line[op.OFFS_OPERATION] == op.READ:
            #Operation is ri(data_item) (read operation by i on data item)
			#retrive data item and transaction id i
            TransactionID = line[op.O_T_ID]
            d_item = line[line.index('(')+1]
            success, TransactionTable, LogTable = read(TransactionID,d_item,TransactionTable,LogTable,\
                                                      Flag_R, fileObject)
            if success == op.ABORT:
				#Come here if because of the read operation transaction got Aborted
                del fileLines[LinesIndex]
                printData = "\nIgnore as T"+ TransactionID +" was Aborted"
				#reinitialize the pointer to operation in file to 0.(Try to execute previous commands that coudn't be executed)
                LinesIndex = 0
                Flag_R = True
            elif success == op.SUCCESS:
				#Come here if read operation(read lock) by transaction was successful
                Flag_R = False
                del fileLines[LinesIndex]
                printData = "\nRead lock acquired by T"+TransactionID+ " on "+d_item
            elif success == op.IGNORE:
				#Come here if operation was ignored because transaction was Blocked(waiting)
                Flag_R = False
                printData = "\nIgnore T"+ TransactionID
				#Increment and get next operation
                LinesIndex += 1
            elif success == op.ABORTED:
                #Come here if transaction was Aborted
                Flag_R = False
                del fileLines[LinesIndex]
                printData = "\nT"+TransactionID+ " was Aborted"
            #write resulting tables to file
            printData += '\n\n'
            printData += str(TransactionTable) + '\n' + str(LogTable) + op.DLine
            printToFile(printData, fileObject)

        elif line[op.OFFS_OPERATION] == op.WRITE:
			#Operation is wi(data_item) (write operation by i on data item)
			#retrive data item and transaction id i
            TransactionID = line[op.O_T_ID]
            d_item = line[line.index('(')+1]
            success, TransactionTable, LogTable = write(TransactionID, d_item, Flag_R,
                                                       TransactionTable, LogTable, fileObject)
            if success == op.ABORT:
				#Come here if because of the write operation transaction got Aborted
                del fileLines[LinesIndex]
                Flag_R = True
                printData = "\nIgnore as T"+ TransactionID +"was Aborted"
				#reinitialize the pointer to operation in file to 0.(Try to execute previous commands that coudn't be executed)
                LinesIndex = 0
                #print(fileLines)
            elif success == op.SUCCESS:
				#Come here if read operation(read lock) by transaction was successful
                Flag_R = False
                del fileLines[LinesIndex]
                printData = "\nWrite lock acquired by T"+TransactionID+ " on "+d_item
            elif success == op.IGNORE:
                #Come here if operation was ignored because transaction was Blocked(waiting)
                Flag_R = False
                printData = "\nIgnore T"+ TransactionID
				#Increment and get next operation
                LinesIndex += 1
            elif success == op.ABORTED:
                #Come here if transaction was Aborted


                Flag_R = False
                del fileLines[LinesIndex]
                printData = "\nT"+TransactionID+ " was Aborted"
			#write resulting tables to file
            printData += '\n\n'
            printData += str(TransactionTable) + '\n' + str(LogTable) + op.DLine
            printToFile(printData, fileObject)

        elif line[op.OFFS_OPERATION] == op.END:
            #Operation is ei (end (commit) operation of transaction i)
			#retrive transaction id i
            TransactionID = line[op.O_T_ID]
            success, TransactionTable, LogTable = commit(TransactionID, TransactionTable, LogTable, fileObject)
            if success == op.SUCCESS:
				#Come here if successfully commited
                del fileLines[LinesIndex]
                Flag_R = True
                printData = "\nT"+TransactionID+" Committed"
				#reinitialize the pointer to operation in file to 0.(Try to execute previous commands that coudn't be executed)
                LinesIndex = 0
            elif success == op.IGNORE:
				#Come here if transaction is Blocked(waiting) already
				#increment operation pointer
                LinesIndex += 1
                Flag_R = False
                printData = "\nIgnore T"+ TransactionID
            elif success == op.ABORTED:
                #Come here if transaction was Aborted
                Flag_R = False
                del fileLines[LinesIndex]
                printData = "\nT"+TransactionID+ " was Aborted"
			#write resulting tables to file
            printData += '\n\n'
            printData += str(TransactionTable) + '\n' + str(LogTable) + op.DLine
            printToFile(printData, fileObject)
		#get new operation, LinesIndex is modified pointer to operations
        line = fileLines[LinesIndex]
    #Write final result to the file
    printData = op.DLine+ '\n'
    printData += op.DLine + '\n' + str(TransactionTable) + op.DLine + '\n' + str(LogTable) + op.DLine + '\n'
    printToFile(printData, fileObject)
    closeOutputFile(fileObject)
    return

if __name__ == '__main__':
    main(sys.argv)
