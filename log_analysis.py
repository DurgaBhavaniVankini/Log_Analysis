#!/usr/bin/env python3

import psycopg2
db_name = "news"


def run_query(query):
    """Connects to the database, runs the query passed to it,
    and returns the results"""
    dbconnect = psycopg2.connect('dbname=' + db_name)
    c = dbconnect.cursor()
    c.execute(query)
    data = c.fetchall()
    dbconnect.close()
    return data


def most_popular_articles():
    """What are the most popular three articles of all time?"""
query = """
    SELECT articles.title, COUNT(*) AS total FROM articles JOIN log
    ON log.path like concat('/article/%', articles.slug)
    GROUP BY articles.title
    ORDER BY total DESC
    LIMIT 3;
"""
# Run Query
try:
    results = run_query(query)

except Exception, e:
    print("Database Error:"+e)


""" Print the Results"""
print('\n The Results are:')
print("\n1. What are the most popular three articles of all time?")
print("\n..................................")
print(' Top Three Articles By Page Views :')
print("....................................")
count = 1
for i in results:
    number = '(' + str(count) + ') "'
    title = i[0]
    views = '" ==>' + str(i[1]) + " views"
    print(number + title + views)
    count += 1


def get_popular_authors():
    """ Who are the most popular article authors of all time?"""
query = """
        SELECT authors.name, COUNT(*) AS total
        FROM authors
        JOIN articles
        ON authors.id = articles.author
        JOIN log
        ON log.path like concat('/article/%', articles.slug)
        GROUP BY authors.name
        ORDER BY total DESC
        LIMIT 3;
    """
# Run Query
try:
    results = run_query(query)

except Exception, e:
    print("Database Error:"+e)

""" Print the Results"""
print("\n2. Who are the most popular article authors of all time?")
print("\n.....................................")
print(' Top Three Authors By Views :')
print(".......................................")
count = 1
for i in results:
    number = '(' + str(count) + ') "'
    title = str(i[0])
    views = '" ==>' + str(i[1]) + " views"
    print(number + title + views)
    count += 1


def days_error_percentage():
    """ On which days did more than 1% of requests lead to errors"""
query = """
        SELECT total.day,
        ROUND(((errors.error_requests*1.0) / total.requests), 3) AS Percent
        FROM (
        SELECT date_trunc('day', time) "day", count(*) AS Error_Requests
        FROM log
        WHERE status LIKE '404%'
        GROUP BY day
        ) AS errors
        JOIN (
        SELECT date_trunc('day', time) "day", count(*) AS Requests
        FROM log
        GROUP BY day
        ) AS total
        ON total.day = errors.day
        WHERE (ROUND(((errors.Error_Requests*1.0) / total.Requests), 3) > 0.01)
        ORDER BY Percent DESC;
    """
# Run Query
try:
    results = run_query(query)

except Exception, e:
    print("Database error:"+e)

""" Print the Results"""
print("\n3. On which days did more than 1% of requests lead to errors")
print("\n..................................")
print(' Days With More Than 1% Errors:')
print("....................................")
count = 1
for i in results:
    date = i[0].strftime('%B, %d, %Y')
    per = str(round(i[1]*100, 1))
    print(date + " ==> " + per + "%" + " errors")
    count += 1
if __name__ == '__main__':
    most_popular_articles()
    get_popular_authors()
    days_error_percentage()
