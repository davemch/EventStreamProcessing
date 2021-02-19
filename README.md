# GDELT Event Processing with CEP

The goal of the present work was to use Apache Flink to examine the data streams of news for social unrest events from all over the world and to visualize them in a web application.  The event database of the GDELT project was used, which contains the categorization of events according to the CAMEO taxonomy. Due to the large volume of data, only the events from 04/25/2020 to 09/25/2020 from the US were considered, a period in which the Black Lives Matter movement was intense.
The data flow is described in Figure 1

<center><img src="https://github.com/davemch/EventStreamProcessing/blob/master/docs/architecture.png" width="600"></center>
<center>Figure 1. Architecture.</center>

## JSON Objects send to kafka

```json
{
    "eventDescription": "{appeal, refuse etc}",
    "eventCode": "010",
    "date": "1600898400000",
    "numMentions": "2",
    "a1Lat": "26.358700",
    "a1Long": "-80.083100",
    "avgTone": "0.843882"
}
```
```json
{
    "eventDescription": "{eruption, refuse etc}_aggregate",
    "startDate": "1585180800000",
    "endDate": "1585180800000",
    "amount": "1"
}
```
```json
{
    "eventDescription": "{refuse, eruption etc}_WARNING",
    "startDate": "1586995200",
    "endDate": "1587600000"
}
```
```json
{
    "eventDescription": "WARNING",
    "startDate": "1586995200",
    "endDate": "1587600000"
}
```
