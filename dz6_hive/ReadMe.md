## Решаем задачи из домашнего задания к уроку 12 RDD/Dataframe/Dataset

### Создаем docker контейнер для работы с hive

    docker-compose up

### Переносим данные в созданный контейнер

    docker cp airports.csv <user id>:/home/airports.csv
    docker cp flights.csv <user id>:/home/flights.csv

### Подключемся к терминалу контейнера и проверяем наличие данных

    docker exec -it <id контейнера> /bin/bash
    ls /home

### Переходим в hive
    hive

    create database otus;

    CREATE EXTERNAL TABLE otus.flights (
        DayofMonth INT,
        DayOfWeek INT,
        Carrier STRING,
        OriginAirportID INT,
        DestAirportID INT,
        DepDelay INT,
        ArrDelay INT,
        FlightId INT
    )
    STORED AS textfile
    LOCATION ‘/home/flights.csv’;


    CREATE EXTERNAL TABLE otus.airports (
        airport_id INT, 
        city STRING, 
        state STRING, 
        name STRING
    ) 
    STORED AS textfile 
    LOCATION ‘/home/airports.csv’;

### Чтобы select печатал схему данных
    set hive.cli.print.header=true;

### Проверяем данные

    SELECT * FROM otus.taxi_facts LIMIT 10;
    SELECT * FROM otus.taxi_zones LIMIT 10;

### Задание 1. Определить аэропорт с максимальной задержкой рейса.

    CREATE VIEW otus.max_depdelay_airports AS 
    SELECT ap.name AS airport_name, max(fl.depdelay) AS max_depdelay 
    FROM otus.flights fl;

### Задание 2. Из каких аэропортов совершается больше рейсов.

    CREATE VIEW otus.popular_airports AS 
    SELECT ap.name AS airport_name, count(flightid) AS num_pickups 
    FROM otus.flights fl 
    JOIN otus.airports ap ON(fl.originairportid = ap.airport_id) 
    GROUP BY ap.name 
    ORDER BY num_pickups DESC;

### Задание 3. В какие дни совершается больше рейсов из Далласа.
    CREATE VIEW otus.popular_days_in_Dallas AS 
    SELECT fl.dayofweek, count(flightid) AS cnt_at_day 
    FROM otus.flights fl 
    JOIN otus.airports ap ON(fl.originairportid = ap.airport_id) 
    WHERE ap.name = 'Dallas' 
    GROUP BY fl.dayofweek 
    ORDER BY cnt_at_day DESC;
   
### Задание 4. В каких аэропортах наибольшее среднее время задержки рейсов.

    CREATE VIEW otus.avg_delay AS 
    SELECT ap.name AS airport_name, avg(fl.depdelay) AS avg_depdelay, avg(fl.arrdelay) AS avg_arrdelay 
    FROM otus.flights fl 
    JOIN otus.airports ap ON(fl.originairportid = ap.airport_id) 
    GROUP BY ap.name 
    ORDER BY avg_depdelay, avg_arrdelay DESC;

### Задание 5. В каких State совершается больше всего внутренних рейсов.
    CREATE VIEW otus.max_orders_in_country AS 
    SELECT apOrg.state, COUNT(flightid) AS flights_cnt 
    FROM otus.flights fl 
    JOIN otus.airports apOrg ON(fl.originairportid = ap.airport_id)
    JOIN otus.airports apDst ON(fl.destairportid = ap.airport_id) 
    WHERE apOrg.state = apDst.state 
    GROUP BY apOrg.state 
    ORDER BY flights_cnt DESC;
