from ibapi.client import EClient
# from ibapi import wrapper
# from ibapi.utils import iswrapper
from ibapi.common import *
from ibapi.wrapper import EWrapper
from ibapi.contract import *

# My non-ibapi imports:
from datetime import datetime, timedelta
from typing import List
import math
import matplotlib.pyplot as plt
import matplotlib.ticker as plticker  # FOr trying to use axis locator, uncluttered x axis
from time import sleep


durationArray = [("S", timedelta(seconds=1)), ("D", timedelta(days=1)), ("W", timedelta(weeks=1)), ("M", timedelta(days=30)), ("Y", timedelta(days=365))]

outerSwitch = {
        "secs": timedelta(seconds=1),
        "mins": timedelta(minutes=1),
        "hours": timedelta(hours=1),
        "day": timedelta(days=1),
        "week": timedelta(weeks=1),
        "month": timedelta(days=30),
        "S": timedelta(seconds=1),
        "D": timedelta(days=1),
        "W": timedelta(weeks=1),
        "M": timedelta(days=30),
        "Y": timedelta(days=365)
    }

def getIndContract(symbol: str, exchange: str, currency: str):
    contract = Contract()
    contract.secType = "IND"
    contract.symbol = symbol
    contract.exchange = exchange
    contract.currency = currency
    return contract

endDate = datetime.now()  # Set first endDate for all requests as current time, at program start
currEndDate = endDate
histDataTypeSetting = "TRADES"
barSizeSetting = "1 day"
reqBarsLimit = 1000
pacingTime = 10.001

timeList = ["180 M"] # Here a sample of about 15 years, may add times as required to collect data
indList = []
iterateCount = 0

dowContract = getIndContract("INDU", "CME", "USD")
indList.append(dowContract)
spxContract = getIndContract("SPX", "CBOE", "USD")
indList.append(spxContract)
daxContract = getIndContract("DAX", "DTB", "EUR")
indList.append(daxContract)

dateDoneNumList = []
dateDoneCountList = []
barCollection = []
reqIdList = []
# Initialize lists that determine when data collection is complete, same size as index list
for ind in indList:
    dateDoneCountList.append(0)
    dateDoneNumList.append(0)
    barCollection.append([])
    reqIdList.append([])


# Converts barSize or duration string into its multiplier and corresponding delta
def splitTime(time: str):
    mySplitTime = time.split()

    # First number (0) split up in time list is the multiplier:int
    mySplitTime[0] = int(mySplitTime[0])

    timeStrDelta = outerSwitch.get(mySplitTime[1], "wrong time entered, see switch in splitTime()")
    # Now combine the multiplier and the delta into the time array:
    mySplitTime[1] = timeStrDelta
    return mySplitTime

# Below returns a delta() time obj for totalDur/totalbarSize calc, and result will be FLOAT
def getTotalDelta(time: str):
    mySplitTime = splitTime(time)
    totalDelta = mySplitTime[0] * mySplitTime[1]
    return totalDelta

# "Parent" function 1 in index loop
def getIterations(duration: str, barSize: str):
    totalDuration = getTotalDelta(duration)
    totalBarSize = getTotalDelta(barSize)
    totalNumBars = totalDuration/totalBarSize

    rawIterations = totalNumBars/reqBarsLimit
    iterations = math.ceil(rawIterations)
    return iterations

# Takes the durationNum and formats it to be used in reqHistoricalData() directly, ex: "1 D" return
def formatDuration(durationNum: int, originalDuration: str):
    durationNumConvert = str(durationNum)
    mySplitTime = originalDuration.split()
    secondDurationPart = mySplitTime[1]  # Get "D" "S", etc from original duration
    formattedDuration = durationNumConvert + " " + secondDurationPart
    return formattedDuration

# Every API historical data request uses a reqId, which we can associate to a particular index
def reqIdtoIndIndex(reqId: int):
    indCounter = 0
    for ind in reqIdList:
        for id in ind:
            if id == reqId:
                return indCounter
        indCounter = indCounter + 1
    print("ERROR: ReqID NOT found in reqIdList")
    return -1  # UNEXPECTED return, reqId should be stored in reqIdList

# Sort bars in descending order by date
def sortBarsDate(barsIndList):
    n = 0
    for ind in barsIndList:
        key = lambda bar: datetime.strptime(bar.date, '%Y%m%d')
        sortedIndBars = sorted(ind, key=key)
        barsIndList[n] = sortedIndBars
        n += 1
    return barsIndList



class MyApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.wasStarted = False
        self.historicalDataCounter = 0

    def error(self, reqId: int, errorCode: int, errorString: str):
        print("Error: ", reqId, "-", errorCode, ":", errorString)
        if (errorCode == 162): # Generic "historical market data service error message"
            currIndIndex = reqIdtoIndIndex(reqId)
            currCount = dateDoneCountList[currIndIndex]
            dateDoneCountList[currIndIndex] = currCount + 1

        # Check if program completes, since it could end with errors instead of bars
        if dateDoneCountList == dateDoneNumList:
            self.disconnect()

    def historicalData(self, reqId: int, bar: BarData):
        currIndIndex = reqIdtoIndIndex(reqId)
        copyCurrBar = barCollection[currIndIndex]
        copyCurrBar.append(bar)
        barCollection[currIndIndex] = copyCurrBar

    def historicalDataEnd(self, reqId:int, start:str, end:str):
        currIndIndex = reqIdtoIndIndex(reqId)
        currCount = dateDoneCountList[currIndIndex]
        dateDoneCountList[currIndIndex] = currCount + 1

        # When all data has been determined to be collected, disconnect
        if dateDoneCountList == dateDoneNumList:
            self.disconnect()

    def nextValidId(self, orderId: int):
        self.start()

    def start(self):
        if self.wasStarted:
            return

        self.wasStarted = True

        for duration in timeList:
            self.getBars(duration, barSizeSetting, indList)

    # Major function responsible for getting all bars for given contract list, duration, and barSize
    def getBars(self, duration: str, barSize: str, myIndList: List[Contract]):
        splitDuration = splitTime(duration)
        splitbarSize = splitTime(barSize)
        # First determine if barSize granularity is allowed for the chosen duration time (S, D, etc)
        if splitDuration[1] / splitbarSize[1] > reqBarsLimit:
            print("barSize is too small for duration")

        for ind in range(len(myIndList)):
            global currEndDate
            currEndDate = endDate
            self.greatestCommonBars(duration, barSize, ind)

    # Function which takes a given duration string (ex 34 D) and converts it to use the
    # greatest possible date units (ex 1 M, 4 D)
    def greatestCommonBars(self, duration, barSize, indCount):
        q, r = 0, 0
        splitDuration = splitTime(duration)
        currDurationIndex = durationArray.index(((duration.split())[1], splitDuration[1]))

        if (currDurationIndex != len(durationArray)-1):  # If already on max duration unit, no need to check further
            nextDurationIndex = currDurationIndex + 1
            nextDurationDelta = durationArray[nextDurationIndex][1]
            currDurationDelta = getTotalDelta(duration)
            q, r = divmod(currDurationDelta, nextDurationDelta)
            if (q != 0):

                movedDurationUnit = durationArray[nextDurationIndex][0]
                movedDuration = str(q) + " " + movedDurationUnit
                self.greatestCommonBars(movedDuration, barSize, indCount)
                # Now check if you need to iterate remainders
                # Putting INSIDE if q!=0, else may have INFINTE LOOP for remainders
                if (r.days != 0):
                    formattedDuration = str(r.days) + " D"
                    self.greatestCommonBars(formattedDuration, barSize, indCount)
                if(r.seconds != 0):
                    formattedDuration = str(r.seconds) + " S"
                    self.greatestCommonBars(formattedDuration, barSize, indCount)
                # Have gone through necessary recursion checks, DO NOT iterate after this recursion done
                return
        global iterateCount  # May remove global var later if not needed
        self.iterateBars(duration, barSize, indCount)
        iterateCount = iterateCount + 1

    def iterateBars(self, duration, barSize, indCount):
        global currEndDate
        splitDuration = splitTime(duration)
        iterations = getIterations(duration, barSize)

        # the int part used in each duration for reqHistoricalData() iteration
        durationStep = math.ceil(splitDuration[0] / iterations)
        formattedDurationStep = formatDuration(durationStep, duration)  # Acceptable format for reqHistorialData()

        # Main for loop that will iterate through contracts list and paste requests until all requests
        # made per calculations above
        for x in range(iterations):
            # If statement for the last iteration, when you request remaining duration
            if (x == iterations - 1):
                durationStep = splitDuration[0] - (durationStep * x)
                if durationStep <= 0:  # You are done iterating; prevents durationStep=0 or negative
                    break
                formattedDurationStep = formatDuration(durationStep, duration)
            myEndDate = currEndDate - (getTotalDelta(formattedDurationStep) * x)
            formattedEndDate = myEndDate.strftime("%Y%m%d %H:%M:%S")

            currReqIdPartition = reqIdList[indCount]
            currReqIdPartition.append(self.historicalDataCounter)
            reqIdList[indCount] = currReqIdPartition
            self.reqHistoricalData(self.historicalDataCounter, indList[indCount], formattedEndDate, formattedDurationStep, barSize, histDataTypeSetting, 1,
                                   1, False, [])
            currDoneNum = dateDoneNumList[indCount]
            # As reqHistoricalData keeps being called, update historical data requests counter
            dateDoneNumList[indCount] = currDoneNum + 1
            sleep(pacingTime)  # Sleep 10.001 seconds for about 1 req/10 sec to satisfy 60 req/10 mins pacing & others
            self.historicalDataCounter = self.historicalDataCounter + 1
        myEndDate = myEndDate - getTotalDelta(formattedDurationStep)  # Move endDate one more step after done with iteration loop above
        # Move endDate for next recursion (if needed)
        currEndDate = myEndDate


def main():
    app = MyApp()
    app.connect("127.0.0.1", 4002, 0)
    app.run()

    print("--- DATA HAS BEEN COLLECTED: ---\n")

    # Print bars in the end
    i = 0
    for bars in barCollection:
        print(indList[i].symbol, "bars:")
        print(bars, "\n")

    print("\n --- DATA END---")


main()
