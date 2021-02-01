# GDELT Event Processing with CEP

## TODOs

- [X] How many fields does each line in a GDELT csv file has? 57 or 58?
  * Seems to be 61...

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
