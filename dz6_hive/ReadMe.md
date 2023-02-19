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
        ArrDelay INT
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

### Задание 1. Из каких аэропортов совершается больше рейсов.

    CREATE VIEW otus.popular_airports AS 
    SELECT ap.name AS airport_name, count(ap.name) AS num_pickups 
    FROM otus.flights fl 
    JOIN otus.airports ap ON(fl.originairportid = ap.airport_id) 
    GROUP BY ap.name 
    ORDER BY num_pickups DESC;

    SELECT * FROM otus.popular_airports;

### Задание 2. В какие дни совершается больше рейсов из Далласа.
    CREATE VIEW otus.popular_days_in_Dallas AS 
    SELECT fl.dayofweek, count(fl.dayofweek) AS cnt_at_day 
    FROM otus.flights fl 
    JOIN otus.airports ap ON(fl.originairportid = ap.airport_id) 
    WHERE ap.name = 'Dallas' 
    GROUP BY fl.dayofweek 
    ORDER BY cnt_at_day DESC;
   
### Задание 3. В каких аэропортах наибольшее среднее время задержки рейсов.

    CREATE VIEW otus.avg_delay AS 
    SELECT ap.name AS airport_name, avg(fl.depdelay) AS avg_depdelay, avg(fl.arrdelay) AS avg_arrdelay 
    FROM otus.flights fl 
    JOIN otus.airports ap ON(fl.originairportid = ap.airport_id) 
    GROUP BY ap.name 
    ORDER BY avg_depdelay, avg_arrdelay DESC;

