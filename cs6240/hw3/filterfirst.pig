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

Flights_Filtered = FILTER Flights BY
    Cancelled == 0 AND Diverted == 0 AND
    ((Year == 2007 AND Month >= 6) OR (Year == 2008 AND Month <= 5));

Flights1_Clean = FOREACH Flights_Filtered GENERATE
    FlightDate AS FlightDate1,
    Dest AS JoinAirport,
    ArrTime AS ArrTime1,
    ArrDelayMinutes AS ArrDelayMinutes1;

Flights2_Clean = FOREACH Flights_Filtered GENERATE
    FlightDate AS FlightDate2,
    Origin AS JoinAirport,
    DepTime AS DepTime2,
    DepDelayMinutes AS DepDelayMinutes2;

JoinedData = JOIN Flights1_Clean BY (FlightDate1, JoinAirport), Flights2_Clean BY (FlightDate2, JoinAirport);

ValidJoinedData = FILTER JoinedData BY DepTime2 > ArrTime1;

FinalData = FOREACH ValidJoinedData GENERATE ArrDelayMinutes1 + DepDelayMinutes2 AS TotalDelay;

GroupedData = GROUP FinalData ALL;
AverageDelay = FOREACH GroupedData GENERATE AVG(FinalData.TotalDelay) AS AvgDelay;

STORE AverageDelay INTO 's3://hw3cs6240/output/filterfirst';