REGISTER 's3://hw3cs6240/piggybank-0.17.0.jar';
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

Flights = LOAD 's3://hw3cs6240/data.csv' USING CSVLoader AS (
    Year:int,
    Quarter:int,
    Month:int,
    DayofMonth:int,
    DayOfWeek:int,
    FlightDate:chararray,
    UniqueCarrier:chararray,
    AirlineID:int,
    Carrier:chararray,
    TailNum:chararray,
    FlightNum:chararray,
    Origin:chararray,
    OriginCityName:chararray,
    OriginState:chararray,
    OriginStateFips:chararray,
    OriginStateName:chararray,
    OriginWac:int,
    Dest:chararray,
    DestCityName:chararray,
    DestState:chararray,
    DestStateFips:chararray,
    DestStateName:chararray,
    DestWac:int,
    CRSDepTime:int,
    DepTime:int,
    DepDelay:double,
    DepDelayMinutes:double,
    DepDel15:double,
    DepartureDelayGroups:int,
    DepTimeBlk:chararray,
    TaxiOut:double,
    WheelsOff:int,
    WheelsOn:int,
    TaxiIn:double,
    CRSArrTime:int,
    ArrTime:int,
    ArrDelay:double,
    ArrDelayMinutes:double,
    ArrDel15:double,
    ArrivalDelayGroups:int,
    ArrTimeBlk:chararray,
    Cancelled:double,
    CancellationCode:chararray,
    Diverted:double,
    CRSElapsedTime:double,
    ActualElapsedTime:double,
    AirTime:double,
    Flights:double,
    Distance:double,
    DistanceGroup:int,
    CarrierDelay:double,
    WeatherDelay:double,
    NASDelay:double,
    SecurityDelay:double,
    LateAircraftDelay:double
);

Flights_Filtered = FILTER Flights BY Cancelled == 0.0 AND Diverted == 0.0;

Flights1 = FOREACH Flights_Filtered GENERATE
    Year AS Year1,
    Month AS Month1,
    FlightDate AS FlightDate1,
    Dest AS JoinAirport,
    ArrTime AS ArrTime1,
    ArrDelayMinutes AS ArrDelayMinutes1;

Flights2 = FOREACH Flights_Filtered GENERATE
    Year AS Year2,
    Month AS Month2,
    FlightDate AS FlightDate2,
    Origin AS JoinAirport,
    DepTime AS DepTime2,
    DepDelayMinutes AS DepDelayMinutes2;

JoinedData = JOIN Flights1 BY (FlightDate1, JoinAirport), Flights2 BY (FlightDate2, JoinAirport);

ValidJoinedData = FILTER JoinedData BY DepTime2 > ArrTime1;

DateFilteredData = FILTER ValidJoinedData BY
    ((Year1 == 2007 AND Month1 >= 6) OR (Year1 == 2008 AND Month1 <= 5)) AND
    ((Year2 == 2007 AND Month2 >= 6) OR (Year2 == 2008 AND Month2 <= 5));

FinalData = FOREACH DateFilteredData GENERATE ArrDelayMinutes1 + DepDelayMinutes2 AS TotalDelay;

GroupedData = GROUP FinalData ALL;
AverageDelay = FOREACH GroupedData GENERATE AVG(FinalData.TotalDelay) AS AvgDelay;

STORE AverageDelay INTO 's3://hw3cs6240/output/joinfirst_v1';