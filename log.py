#!/usr/bin/env python3

import psycopg2
database = "news"


def run(query):
    try:
        connection = psycopg2.connect("dbname=" + database)
        conn = connection.cursor()
    except Exception as e:
        print("unable to connect error: %s " % e)
    else:
        conn.execute(query)
        output = conn.fetchall()
        connection.close()
        return output


''' Definition to run query 1 '''


def execute_query1():
    '''Query to get most popular 3 articles'''
    q1 = """
    SELECT title, count(*) as cnt from articles JOIN log ON
    log.path=concat('/article/', articles.slug)
    GROUP BY title ORDER BY cnt DESC limit 3;
    """
    output = run(q1)
    print('\n Results:')
    print("\n1. What are the most popular three articles of all time?")
    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print('Top 3 Articles')
    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    for i in output:
        print(str(i[0]) + ':' + str(i[1]) + ' views ')


''' Definition to run query 2 '''


def execute_query2():
    '''Query to get the most popular article authors'''
    q2 = """
    SELECT name, count(*) as cnt from authors JOIN articles
    ON articles.author=authors.id JOIN log
    ON log.path=concat('/article/', articles.slug)
    GROUP BY name ORDER BY cnt DESC limit 3;
    """
    output = run(q2)
    print("\n2. Who are the most popular article authors of all time?")
    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print('Most Popular Authors')
    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    for i in output:
        print(str(i[0]) + ':' + str(i[1]) + ' views ')


''' Definition to run query 3 '''


def execute_query3():
    q3 = """
         SELECT total.month, total.day, total.year,
    ROUND(((err.er*1.0) / total.r), 3) AS P
            FROM (
              SELECT date_part('month', time) "month",
              date_part('day', time) "day",
              date_part('year', time) "year", count(*) AS ER
              FROM log
              WHERE status LIKE '404%'
              GROUP BY day, month, year
            ) AS err
            JOIN (
              SELECT date_part('month', time) "month",
              date_part('day', time) "day",
              date_part('year', time) "year", count(*) AS R
              FROM log
              GROUP BY day, month, year
              ) AS total
            ON total.day = err.day and total.year = err.year
            and total.month = err.month
            WHERE (ROUND(((err.ER*1.0) / total.R), 3) > 0.01)
            ORDER BY P DESC;
            """
    output = run(q3)
    print("\n3. On which days did more than 1% of requests lead to errors?")
    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print('Error Percentage:')
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    for f in output:
        percentage = str(f[3]*100)
        m = int(f[0])
        d = int(f[1])
        y = int(f[2])
        print(str(m) + "-" + str(d) + "-" + str(y) +
              " -->" + percentage + "%" + " errors")


if __name__ == '__main__':
    execute_query1()
    execute_query2()
    execute_query3()
