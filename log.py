#!/usr/bin/env python3

import psycopg2
database = "news"


def run(query):
    connection = psycopg2.connect("dbname=" + database)
    conn = connection.cursor()
    conn.execute(query)
    output = conn.fetchall()
    connection.close()
    return output

'''Query to get most popular 3 articles'''
q1 = """
     SELECT title, count(*) as cnt from articles JOIN log ON
     log.path=concat('/article/', articles.slug)
     GROUP BY title ORDER BY cnt DESC limit 3;
"""

'''Query to get the most popular article authors'''
q2 = """
SELECT name, count(*) as cnt from authors JOIN articles
ON articles.author=authors.id JOIN log
ON log.path=concat('/article/', articles.slug)
GROUP BY name ORDER BY cnt DESC limit 3; """

'''Query to get Days with more than 1% of requests leads to errors'''
q3 = """
     SELECT day, percentage from (
     select day, round((sum(requests)/(select count(*) from log where
     substring(cast(log.time as text), 0, 11) = day) * 100), 2) as
     percentage from (select substring(cast(log.time as text), 0, 11) as day,
     count(*) as requests from log where status like '%404%' group by day)
     as log_percentage group by day order by percentage desc) as final_query
     where percentage >= 1
      """


''' Definition to run query 1 '''


def execute_query1(query):

        output = run(query)
        print('\n Results:')
        print("\n1. What are the most popular three articles of all time?")
        print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        print('Top 3 Articles')
        print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        for i in output:
            print(str(i[0]) + ':' +
                  str(i[1]) + ' views ')

''' Definition to run query 2 '''


def execute_query2(query):
    output = run(query)
    print("\n2. Who are the most popular article authors of all time?")
    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print('Most Popular Authors')
    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    for i in output:
        print(str(i[0]) + ':' +
              str(i[1]) + ' views ')

''' Definition to run query 3 '''


def execute_query3(query):
    output = run(query)
    print("\n3. On which days did more than 1% of requests lead to errors?")
    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print('Error Percent')
    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    for i in output:
        print(str(i[0]) + ':' + str(i[1]) + ' % errors ')

if __name__ == '__main__':
    execute_query1(q1)
    execute_query2(q2)
    execute_query3(q3)
