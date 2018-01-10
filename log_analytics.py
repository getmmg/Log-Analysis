#!python3
import psycopg2


def run_query(query):
    '''
    This function takes the sql query returns output
    Arguments:
    1. Query to run on News Table
    Returns:
    All rows returned from query
    '''
    db = psycopg2.connect("dbname=news")
    c = db.cursor()
    c.execute(query)
    rows = c.fetchall()
    db.close()
    return rows


def print_results(question, results, result_type):
    '''
    This function takes the sql results and prints it
    Arguments:
    1. Question
    2. Query Results
    3. Result Type (View or Percent)
    Returns:
    None
    '''
    index = 0
    print("Question : {}".format(question))
    print("Answer :")
    for row in results:
        index += 1
        print('{}. {}\t - {} {}'.format(
            str(index),
            row[0],
            str(row[1]),
            result_type))
    print("")


if __name__ == '__main__':
    question1 = "What are the most popular three articles of all time?"
    query1 = ("select articles.title, count(*) as views from articles "
              "left join log on log.path like concat('%', articles.slug, '%') "
              "group by articles.title,log.path order by views  desc limit 3;")
    question2 = "Who are the most popular article authors of all time?"
    query2 = ("select authors.name, count(*) as views from articles "
              "left join authors on articles.author = authors.id "
              "left join log on log.path like concat('%', articles.slug, '%') "
              "where log.status like '%200%' group by authors.name "
              "order by views desc;")
    question3 = "On which days did more than 1% of requests lead to errors?"
    query3 = ("select total.day, "
              "round("
              "(errors.error_requests * 1.0 / total.requests), 4) "
              "* 100 as fail_percent "
              "from (select date(time) as day, count(*) as error_requests "
              "from log where status like '%404%' group by day) as errors "
              "join (select date(time) as day, count(*) as requests "
              "from log group by day) as total on total.day = errors.day "
              "where (round"
              "((errors.error_requests * 1.0 / total.requests), 4) "
              "* 100) > 1;")
    print_results(question1, run_query(query1), "views")
    print_results(question2, run_query(query2), "views")
    print_results(question3, run_query(query3), "% errors")
