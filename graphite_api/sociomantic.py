# coding: utf-8
# Copyright 2008 Orbitz WorldWide
# Copyright 2014 Bruno Renié
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from six.moves import zip, zip_longest, reduce
from operator import itemgetter
from itertools import repeat

from .render.datalib import TimeSeries
from .functions import formatPathExpressions, normalize, lcm, safeSum, safeDiv, safeAvg, safeLen, safeMul,\
    movingMedian, movingAverage

NAN = float('NaN')
INF = float('inf')
MINUTE = 60
HOUR = MINUTE * 60
DAY = HOUR * 24


def normalizeApproximate(seriesLists):
    seriesList = reduce(lambda L1, L2: L1+L2, seriesLists)
    step = reduce(lcm, [s.step for s in seriesList])
    for s in seriesList:
         s.approximate(s.step // step)
    start = min([s.start for s in seriesList])
    end = max([s.end for s in seriesList])
    end -= (end - start) % step
    return seriesList, start, end, step


def safeNoneSum(values):
    for v in values:
        if v is None:
            return None
    return sum(values)


def divideSeries(requestContext, dividendSeriesList, divisorSeriesList):
    """
    Takes a dividend metric and a divisor metric and draws the division result.
    A constant may *not* be passed. To divide by a constant, use the scale()
    function (which is essentially a multiplication operation) and use the
    inverse of the dividend. (Division by 8 = multiplication by 1/8 or 0.125)

    Example::

        &target=divideSeries(Series.dividends,Series.divisors)
        &target=divideSeries(Server*.connections.{failed,succeeded},
                             Server*.connections.attempted)


    """
    results = list()
    if len(divisorSeriesList) != 1 and len(divisorSeriesList) != len(dividendSeriesList):
        raise ValueError(
            "divideSeries arguments must have the same length (%s != %s)" %
            (len(dividendSeriesList), len(divisorSeriesList)))

    if len(divisorSeriesList) == 1:
        divisorSeriesList = repeat(divisorSeriesList[0],
                                   len(dividendSeriesList))

    for dividendSeries, divisorSeries in zip_longest(dividendSeriesList, divisorSeriesList):
        name = "divideSeries(%s,%s)" % (dividendSeries.name,
                                        divisorSeries.name)
        bothSeries = (dividendSeries, divisorSeries)
        step = reduce(lcm, [s.step for s in bothSeries])

        for s in bothSeries:
            s.consolidate(step / s.step)

        start = min([s.start for s in bothSeries])
        end = max([s.end for s in bothSeries])
        end -= (end - start) % step

        values = (safeDiv(v1, v2) for v1, v2 in zip_longest(*bothSeries))

        quotientSeries = TimeSeries(name, start, end, step, values)
        quotientSeries.pathExpression = name
        results.append(quotientSeries)

    return results


def asPercent(requestContext, seriesList, total=None):
    """

    Calculates a percentage of the total of a wildcard series. If `total` is
    specified, each series will be calculated as a percentage of that total.
    If `total` is not specified, the sum of all points in the wildcard series
    will be used instead.

    The `total` parameter may be a list of series, a single series or a numeric value.

    Example::

        &target=asPercent(Server01.connections.{failed,succeeded},
                          Server01.connections.attempted)
        &target=asPercent(Server*.connections.{failed,succeeded},
                          Server*.connections.attempted)
        &target=asPercent(apache01.threads.busy,1500)
        &target=asPercent(Server01.cpu.*.jiffies)

    """
    if not seriesList:
        return []
    normalizeApproximate([seriesList])

    if total is None:
        totalValuesList = repeat([safeSum(row) for row in zip_longest(*seriesList)], len(seriesList))
        totalTextList = repeat(None, len(seriesList))  # series.pathExpression
    elif isinstance(total, list):
        if len(total) == 0:
            raise ValueError(
                "asPercent total series are empty (maybe you have a typo?)")
        if len(total) == 1:
            total = [total[0]] * len(seriesList)
        elif len(total) != len(seriesList):
            raise ValueError(
                "asPercent arguments must have the same length (%s != %s)" %
                (len(seriesList), len(total)))
        normalizeApproximate([seriesList, total])
        totalValuesList = total
        totalTextList = ( t.name for t in totalValuesList )

    else:
        totalValuesList = repeat(repeat(total, len(seriesList[0])), len(seriesList))
        totalTextList = repeat(str(total), len(seriesList))

    resultList = []
    for series, totalValues, totalText in zip_longest(seriesList, totalValuesList, totalTextList):
        resultValues = [safeMul(safeDiv(val, totalVal), 100.0)
                        for val, totalVal in zip_longest(series, totalValues)]

        name = "asPercent(%s, %s)" % (series.name,
                                      totalText or series.pathExpression)
        resultSeries = TimeSeries(name, series.start, series.end, series.step,
                                  resultValues)
        resultSeries.pathExpression = name
        resultList.append(resultSeries)

    return resultList


def movingMedianOld(requestContext, seriesList, windowSize):
    """
    Takes one metric or a wildcard seriesList followed by a number N of datapoints and graphs
    the median of N previous datapoints.  N-1 datapoints are set to None at the
    beginning of the graph.

    .. code-block:: none

      &target=movingMedian(Server.instance01.threads.busy,10)

    """
    for seriesIndex, series in enumerate(seriesList):
        newName = "movingMedian(%s,%.1f)" % (series.name, float(windowSize))
        newSeries = TimeSeries(newName, series.start, series.end, series.step, [])
        newSeries.pathExpression = newName

        windowIndex = windowSize - 1

        for i in range( len(series) ):
            if i < windowIndex: # Pad the beginning with None's since we don't have enough data
                newSeries.append( None )
            else:
                window = series[i - windowIndex : i + 1]
                nonNull = [ v for v in window if v is not None ]
                if nonNull:
                    m_index = len(nonNull) / 2
                    newSeries.append(sorted(nonNull)[m_index])
                else:
                    newSeries.append(None)
        seriesList[ seriesIndex ] = newSeries

    return seriesList


def movingAverageOld(requestContext, seriesList, windowSize):
    """
    Takes one metric or a wildcard seriesList followed by a number N of datapoints and graphs
    the average of N previous datapoints.  N-1 datapoints are set to None at the
    beginning of the graph.

    .. code-block:: none

      &target=movingAverage(Server.instance01.threads.busy,10)

    """
    for seriesIndex, series in enumerate(seriesList):
        newName = "movingAverage(%s,%d)" % (series.name, windowSize)
        newSeries = TimeSeries(newName, series.start, series.end, series.step, [])
        newSeries.pathExpression = newName

        windowIndex = int(windowSize) - 1

        for i in range( len(series) ):
            if i < windowIndex: # Pad the beginning with None's since we don't have enough data
                newSeries.append( None )
            else:
                window = series[i - windowIndex : i + 1]
                nonNull = [ v for v in window if v is not None ]
                if nonNull:
                    newSeries.append( sum(nonNull) / len(nonNull) )
                else:
                    newSeries.append(None)

        seriesList[ seriesIndex ] = newSeries

    return seriesList

def sumSeriesWithoutNone(requestContext, *seriesLists):
    """
    Short form: sumWithoutNone()

    This will add metrics together and return the sum at each datapoint. (See
    integral for a sum over time)

    If one of the summed values is None, all values will be None.

    Example:

      &target=sumSeriesWithoutNone(company.server.application*.requestsHandled)

    This would show the sum of all requests handled per minute (provided
    requestsHandled are collected once a minute).   If metrics with different
    retention rates are combined, the coarsest metric is graphed, and the sum
    of the other metrics is averaged for the metrics with finer retention
    rates.

    """
    if not seriesLists or seriesLists == ([],):
        return []
    seriesList, start, end, step = normalize(seriesLists)
    name = "sumSeriesWithoutNone(%s)" % formatPathExpressions(seriesList)
    values = (safeNoneSum(row) for row in zip(*seriesList))
    series = TimeSeries(name, start, end, step, values)
    series.pathExpression = name
    return [series]


def mostDeviant(requestContext, seriesList, n):
    """
    Takes one metric or a wildcard seriesList followed by an integer N.
    Draws the N most deviant metrics.
    To find the deviants, the standard deviation (sigma) of each series
    is taken and ranked. The top N standard deviations are returned.

    Example::

        &target=mostDeviant(server*.instance*.memory.free, 5)

    Draws the 5 instances furthest from the average memory free.
    """

    deviants = []
    realSeriesList = seriesList
    realN = n
    if isinstance(n, list):
        realSeriesList = n
        realN = seriesList
    for series in realSeriesList:
        mean = safeAvg(series)
        if mean is None:
            continue
        square_sum = sum([(value - mean) ** 2 for value in series
                          if value is not None])
        sigma = safeDiv(square_sum, safeLen(series))
        if sigma is None:
            continue
        deviants.append((sigma, series))
    return [series for sig, series in sorted(deviants,  # sort by sigma
                                             key=itemgetter(0),
                                             reverse=True)][:realN]


def fastNormalize(seriesLists):
    step = seriesLists[0].step
    min_step = step
    start = seriesLists[0].start
    end = seriesLists[0].end
    for s in seriesLists:
        if step != s.step:
            step = lcm(step, s.step)
            if step > s.step:
                min_step = s.step
        if start > s.start:
            start = s.start
        if end < s.end:
            end = s.end
#    seriesList = reduce(lambda L1, L2: L1+L2, seriesLists)
#    step = reduce(lcm, [s.step for s in seriesList])
    if step != min_step:
        for s in seriesLists:
            s.consolidate(step // s.step)
    end -= (end - start) % step
    return seriesLists, start, end, step


def safeSumFast(seriesLists):
    l2 = range(len(seriesLists))
    result = list()
    for cnt2 in l2:
        cnt = 0
        for val in seriesLists[cnt2]:
            if val is not None:
                try:
                    result[cnt] += val
                except IndexError:
                    result.append(val)
            cnt += 1
    return result
        
    

def sumSeriesFast(requestContext, *seriesLists):
    """
    Short form: sum()

    This will add metrics together and return the sum at each datapoint. (See
    integral for a sum over time)

    Example::

        &target=sum(company.server.application*.requestsHandled)

    This would show the sum of all requests handled per minute (provided
    requestsHandled are collected once a minute).     If metrics with different
    retention rates are combined, the coarsest metric is graphed, and the sum
    of the other metrics is averaged for the metrics with finer retention
    rates.

    """
    if not seriesLists or seriesLists == ([],):
        return []
    seriesList, start, end, step = fastNormalize(seriesLists[0])
    name = "sumSeriesFast(%s)" % formatPathExpressions(seriesList)
    values = safeSumFast(seriesList)
    series = TimeSeries(name, start, end, step, values)
    series.pathExpression = name
    return [series]


SociomanticSeriesFunctions = {
    # Combine functions
    'divideSeries': divideSeries,
    'asPercent': asPercent,
    'movingAverageNew': movingAverage,
    'movingMedianNew': movingMedian,
    'movingAverage': movingAverageOld,
    'movingMedian': movingMedianOld,
    'sumSeriesWithoutNone': sumSeriesWithoutNone,
    'sumWithoutNone': sumSeriesWithoutNone,
    'sumSeriesFast' : sumSeriesFast,
}
