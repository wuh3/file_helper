REGISTER 's3://hw3cs6240/piggybank-0.17.0.jar';
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

Flights1 = LOAD 's3://hw3cs6240/data.csv' USING CSVLoader AS (
    Year1:int,
    Quarter1:int,
    Month1:int,
    DayofMonth1:int,
    DayOfWeek1:int,
    FlightDate1:chararray,
    UniqueCarrier1:chararray,
    AirlineID1:int,
    Carrier1:chararray,
    TailNum1:chararray,
    FlightNum1:chararray,
    Origin1:chararray,
    OriginCityName1:chararray,
    OriginState1:chararray,
    OriginStateFips1:chararray,
    OriginStateName1:chararray,
    OriginWac1:int,
    Dest1:chararray,
    DestCityName1:chararray,
    DestState1:chararray,
    DestStateFips1:chararray,
    DestStateName1:chararray,
    DestWac1:int,
    CRSDepTime1:int,
    DepTime1:int,
    DepDelay1:double,
    DepDelayMinutes1:double,
    DepDel15_1:double,
    DepartureDelayGroups1:int,
    DepTimeBlk1:chararray,
    TaxiOut1:double,
    WheelsOff1:int,
    WheelsOn1:int,
    TaxiIn1:double,
    CRSArrTime1:int,
    ArrTime1:int,
    ArrDelay1:double,
    ArrDelayMinutes1:double,
    ArrDel15_1:double,
    ArrivalDelayGroups1:int,
    ArrTimeBlk1:chararray,
    Cancelled1:double,
    CancellationCode1:chararray,
    Diverted1:double,
    CRSElapsedTime1:double,
    ActualElapsedTime1:double,
    AirTime1:double,
    Flights1:double,
    Distance1:double,
    DistanceGroup1:int,
    CarrierDelay1:double,
    WeatherDelay1:double,
    NASDelay1:double,
    SecurityDelay1:double,
    LateAircraftDelay1:double
);

Flights2 = LOAD 's3://hw3cs6240/data.csv' USING CSVLoader AS (
    Year2:int,
    Quarter2:int,
    Month2:int,
    DayofMonth2:int,
    DayOfWeek2:int,
    FlightDate2:chararray,
    UniqueCarrier2:chararray,
    AirlineID2:int,
    Carrier2:chararray,
    TailNum2:chararray,
    FlightNum2:chararray,
    Origin2:chararray,
    OriginCityName2:chararray,
    OriginState2:chararray,
    OriginStateFips2:chararray,
    OriginStateName2:chararray,
    OriginWac2:int,
    Dest2:chararray,
    DestCityName2:chararray,
    DestState2:chararray,
    DestStateFips2:chararray,
    DestStateName2:chararray,
    DestWac2:int,
    CRSDepTime2:int,
    DepTime2:int,
    DepDelay2:double,
    DepDelayMinutes2:double,
    DepDel15_2:double,
    DepartureDelayGroups2:int,
    DepTimeBlk2:chararray,
    TaxiOut2:double,
    WheelsOff2:int,
    WheelsOn2:int,
    TaxiIn2:double,
    CRSArrTime2:int,
    ArrTime2:int,
    ArrDelay2:double,
    ArrDelayMinutes2:double,
    ArrDel15_2:double,
    ArrivalDelayGroups2:int,
    ArrTimeBlk2:chararray,
    Cancelled2:double,
    CancellationCode2:chararray,
    Diverted2:double,
    CRSElapsedTime2:double,
    ActualElapsedTime2:double,
    AirTime2:double,
    Flights2:double,
    Distance2:double,
    DistanceGroup2:int,
    CarrierDelay2:double,
    WeatherDelay2:double,
    NASDelay2:double,
    SecurityDelay2:double,
    LateAircraftDelay2:double
);

Flights1_Filtered = FILTER Flights1 BY Cancelled1 == 0 AND Diverted1 == 0;
Flights2_Filtered = FILTER Flights2 BY Cancelled2 == 0 AND Diverted2 == 0;

-- Project necessary fields
Flights1_Clean = FOREACH Flights1_Filtered GENERATE
    Year1, Month1, FlightDate1, Dest1 AS JoinAirport, ArrTime1, ArrDelayMinutes1;

Flights2_Clean = FOREACH Flights2_Filtered GENERATE
    Year2, Month2, FlightDate2, Origin2 AS JoinAirport, DepTime2, DepDelayMinutes2;

-- Join on FlightDate and JoinAirport
JoinedData = JOIN Flights1_Clean BY (FlightDate1, JoinAirport), Flights2_Clean BY (FlightDate2, JoinAirport);

-- Filter where DepTime2 > ArrTime1
ValidJoinedData = FILTER JoinedData BY DepTime2 > ArrTime1;

DateFilteredData = FILTER ValidJoinedData BY
    ( (Year1 == 2007 AND Month1 >= 6) OR (Year1 == 2008 AND Month1 <= 5) );

-- Compute Total Delay
FinalData = FOREACH DateFilteredData GENERATE ArrDelayMinutes1 + DepDelayMinutes2 AS TotalDelay;

-- Compute Average Delay
GroupedData = GROUP FinalData ALL;
AverageDelay = FOREACH GroupedData GENERATE AVG(FinalData.TotalDelay) AS AvgDelay;

-- Store the result
STORE AverageDelay INTO 's3://hw3cs6240/output/joinfirst_v2';