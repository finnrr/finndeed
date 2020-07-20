import flask
import dill
import pandas as pd
import sqlite3
from bs4 import BeautifulSoup
from bs4.element import Comment
from selenium import webdriver
from selenium.webdriver import Firefox
from selenium.webdriver.firefox.options import Options
import urllib.request
import time
import random
import re 
import sqlite3
import nltk
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from nltk.tokenize import RegexpTokenizer
from sklearn.pipeline import make_pipeline 
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn_pandas import DataFrameMapper, FunctionTransformer
from sklearn.pipeline import Pipeline

app = flask.Flask(__name__)
#gecko_path = '/app/vendor/geckodriver/geckodriver'
#firefox_path = '/app/vendor/firefox/firefox'

gecko_path = 'geckodriver'

def return_html(url):
    options = Options()
    options.headless = True
 #   options.binary_location = firefox_path
    browser = Firefox(executable_path=gecko_path, options=options)
    browser.get(url)
    html = browser.page_source
    browser.close()
    return html

@app.route('/')
def index():
    con = sqlite3.connect('jobs.db')
    table = pd.read_sql('''
        SELECT 
        *
        FROM jobs
        ORDER BY like DESC
        ,scraped
        ''', con).to_dict()
    return flask.render_template('index.html', **table)

@app.route('/about')
def about():
    return flask.render_template('about.html')

@app.route('/newjobpage')
def new_job_page():
    con = sqlite3.connect('jobs.db')
    table = pd.read_sql('''
        SELECT 
        *
        FROM new_jobs
        ORDER BY predict DESC
        ''', con).to_dict()
    return flask.render_template('newjobpage.html', **table)

@app.route('/upvote', methods=['POST'])
def upvote():
    args = flask.request.form
    link = args.get('upvote')
    con = sqlite3.connect('jobs.db')
    cursor = con.cursor()
    sql_update_query = f"""Update jobs set like = 1 where link == \'{link}\'"""
    cursor.execute(sql_update_query)
    con.commit()
    cursor.close()
    return index()

@app.route('/downvote', methods=['POST'])
def downvote():
    args = flask.request.form
    link = args.get('downvote')
    print(link)
    con = sqlite3.connect('jobs.db')
    cursor = con.cursor()
    sql_update_query = f"""Update jobs set like = 0 where link == \'{link}\'"""
    cursor.execute(sql_update_query)
    con.commit()
    cursor.close()
    return index()

@app.route('/remove_link', methods=['POST'])
def remove_link():
    args = flask.request.form
    link = args.get('link')
    con = sqlite3.connect('jobs.db')
    cursor = con.cursor()
    sql_update_query =  f'''Delete FROM jobs where "link" == \"{link}\" '''
    cursor.execute(sql_update_query)
    con.commit()
    cursor.close()
    return index()

@app.route('/upvote_new', methods=['POST'])
def upvote_new():
    args = flask.request.form
    link = args.get('upvote')
    con = sqlite3.connect('jobs.db')
    cursor = con.cursor()
    sql_update_query = f"""Update new_jobs set like = 1 where link == \'{link}\'"""
    cursor.execute(sql_update_query)
    con.commit()
    cursor.close()
    return new_job_page()

@app.route('/downvote_new', methods=['POST'])
def downvote_new():
    args = flask.request.form
    link = args.get('downvote')
    print(link)
    con = sqlite3.connect('jobs.db')
    cursor = con.cursor()
    sql_update_query = f"""Update new_jobs set like = 0 where link == \'{link}\'"""
    cursor.execute(sql_update_query)
    con.commit()
    cursor.close()
    return new_job_page()

@app.route('/remove_link_new', methods=['POST'])
def remove_link_new():
    args = flask.request.form
    link = args.get('link')
    con = sqlite3.connect('jobs.db')
    cursor = con.cursor()
    sql_update_query =  f'''Delete FROM new_jobs where "link" == \"{link}\" '''
    cursor.execute(sql_update_query)
    con.commit()
    cursor.close()
    return new_job_page()

def tag_visible(element):
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True

def text_from_html(body):
    soup = BeautifulSoup(body, 'html.parser')
    texts = soup.findAll(text=True)
    visible_texts = filter(tag_visible, texts)  
    return u" ".join(t.strip() for t in visible_texts)

def title_to_tokens(title):
    lower_case = title.lower()
    retokenizer = RegexpTokenizer(r'\w+')
    words = retokenizer.tokenize(lower_case)
    stops = set(stopwords.words('english'))
    meaningful_words = [w for w in words if not w in stops]
    stemmer = PorterStemmer()
    stemmed_words = [stemmer.stem(w) for w in meaningful_words]
    return " ".join(stemmed_words) 

def tokens_from_link(link):
    html = return_html(link)
    text = text_from_html(html)
    tokens = title_to_tokens(text)
    return tokens

@app.route('/retrain', methods=['POST'])
def retrain():
    con = sqlite3.connect('jobs.db')
    sql_query = '''SELECT content, like FROM jobs where (content !=\'\' and like != 9)'''
    df = pd.read_sql(sql_query, con)
    mapper = DataFrameMapper([
        ('content', CountVectorizer())
    ], df_out=True)
    model = LogisticRegression()
    pipe = make_pipeline(mapper, model)
    X = df.drop('like', axis=1)
    y = df['like']
    pipe.fit(X , y)
    dill.dump(pipe, open("pipe.pkl", "wb"))
    return index()

@app.route('/add_link', methods=['POST'])
def add_link():
    args = flask.request.form
    link = args.get('link_new')
    link = link.strip()
    title = args.get('title_new')
    con = sqlite3.connect('jobs.db')
    cursor = con.cursor()
    sql_update_query =  f'''INSERT INTO jobs (link, like, scraped, title) VALUES (\"{link}\", 9, 0, \"{title}\")'''
    cursor.execute(sql_update_query)
    con.commit()
    cursor.close()
    return index()

@app.route('/add_link_new', methods=['POST'])
def add_link_new():
    args = flask.request.form
    link = args.get('link_new')
    link = link.strip()
    title = args.get('title_new')
    con = sqlite3.connect('jobs.db')
    cursor = con.cursor()
    sql_update_query =  f'''INSERT INTO new_jobs (link, like, title) VALUES (\"{link}\", 9, \"{title}\")'''
    cursor.execute(sql_update_query)
    con.commit()
    cursor.close()
    return new_job_page()

@app.route('/scrape_single', methods=['POST'])
def scrape_single():
    args = flask.request.form
    link = args.get('link')
    tokens = tokens_from_link(link)
    con = sqlite3.connect('jobs.db')
    cursor = con.cursor()
    sql_update_query =  f'''Update jobs set content = \"{tokens}\" where "link" == \"{link}\" '''
    cursor.execute(sql_update_query)
    con.commit()
    sql_update_query =  f'''Update jobs set scraped = 1 where "link" == \"{link}\" '''
    cursor.execute(sql_update_query)
    con.commit()
    cursor.close()
    return index()

@app.route('/scrape_single_new', methods=['POST'])
def scrape_single_new():
    args = flask.request.form
    link = args.get('link')
    tokens = tokens_from_link(link)
    con = sqlite3.connect('jobs.db')
    cursor = con.cursor()
    sql_update_query =  f'''Update new_jobs set content = \"{tokens}\",scraped = 1 where "link" == \"{link}\" '''
    cursor.execute(sql_update_query)
    con.commit()
    cursor.close()
    return new_job_page()

@app.route('/refresh_new_jobs', methods=['POST'])
def refresh_new_jobs():
    args = flask.request.form
    search = args.get('search_string')
    url = make_daily_url(search)
    new_jobs(url)
    return new_job_page()

def populate_new_jobs():
    con = sqlite3.connect('jobs.db')
    link_list = pd.read_sql(f"""SELECT link FROM new_jobs where content == \'\'""", con)['link']
    for link in link_list:
        cursor = con.cursor()
        tokens = tokens_from_link(link) 
        sql_update_query = f'''Update new_jobs set content = \'{tokens}\', scraped = 1 where link == \"{link}\"'''
        cursor.execute(sql_update_query)
        con.commit()
        cursor.close()
    con.close()

def predict_new_jobs():
    con = sqlite3.connect('jobs.db')
    token_list = pd.read_sql(f"""SELECT content FROM new_jobs where content != \'\'""", con)['content']
    for content in token_list:
        cursor = con.cursor()
        df = pd.DataFrame({'content': [content]})
        pipe = dill.load(open('pipe.pkl', 'rb'))
        answer_raw = pipe.predict_proba(df)[0][1]
        answer = round(answer_raw *100, 2)
        sql_update_query = f'''Update new_jobs set predict = \'{answer}\' where content == \"{content}\"'''
        cursor.execute(sql_update_query)
        con.commit()
        cursor.close()
    con.close()

@app.route('/predict_single_job', methods=['POST'])
def predict_single_job():
    args = flask.request.form
    link = args.get('link')
    con = sqlite3.connect('jobs.db')
    tokens = pd.read_sql(f"""SELECT content FROM new_jobs where link == \'{link}\'""", con)['content'][0]
    df = pd.DataFrame({'content': [tokens]})
    pipe = dill.load(open('pipe.pkl', 'rb'))
    answer_raw = pipe.predict_proba(df)[0][1]
    answer = round(answer_raw *100, 2)
    cursor = con.cursor()
    sql_update_query =  f'''Update new_jobs set predict = \"{answer}\" where "link" == \"{link}\" '''
    cursor.execute(sql_update_query)
    con.commit()
    cursor.close()
    return new_job_page()

@app.route('/predict_single_dataframe', methods=['POST'])
def predict_single_dataframe():
    args = flask.request.form
    link = args.get('link')
    con = sqlite3.connect('jobs.db')
    tokens = pd.read_sql(f"""SELECT content FROM jobs where link == \'{link}\'""", con)['content'][0]
    df = pd.DataFrame({'content': [tokens]})
    pipe = dill.load(open('pipe.pkl', 'rb'))
    answer_raw = pipe.predict_proba(df)[0][1]
    answer = round(answer_raw *100, 2)
    cursor = con.cursor()
    sql_update_query =  f'''Update jobs set predict = \"{answer}\" where "link" == \"{link}\" '''
    cursor.execute(sql_update_query)
    con.commit()
    cursor.close()
    return index()

@app.route('/scrape_predict_single_job', methods=['POST'])
def scrape_predict_single_job():
    args = flask.request.form
    link = args.get('link')
    tokens = tokens_from_link(link)
    con = sqlite3.connect('jobs.db')
    df = pd.DataFrame({'content': [tokens]})
    pipe = dill.load(open('pipe.pkl', 'rb'))
    answer_raw = pipe.predict_proba(df)[0][1]
    answer = round(answer_raw *100, 2)
    cursor = con.cursor()
    sql_update_query =  f'''Update new_jobs set predict = \"{answer}\" where "link" == \"{link}\" '''
    cursor.execute(sql_update_query)
    con.commit()
    sql_update_query =  f'''Update new_jobs set content = \"{tokens}\" where "link" == \"{link}\" '''
    cursor.execute(sql_update_query)
    con.commit()
    sql_update_query =  f'''Update new_jobs set scraped = "1" where "link" == \"{link}\" '''
    cursor.execute(sql_update_query)
    con.commit()
    cursor.close()
    return new_job_page()

@app.route('/generate_predictions', methods=['POST'])
def generate_predictions():
    populate_new_jobs()
    predict_new_jobs()
    return new_job_page()

@app.route('/transfer', methods=['POST'])
def transfer():
    args = flask.request.form
    link = args.get('link')
    con = sqlite3.connect('jobs.db')
    cursor = con.cursor()
    content = pd.read_sql(f'''SELECT content FROM new_jobs WHERE "link" == \"{link}\"''', con)['content'][0]
    scraped = int(pd.read_sql(f'''SELECT scraped FROM new_jobs WHERE "link" == \"{link}\"''', con)['scraped'][0])
    like = pd.read_sql(f'''SELECT like FROM new_jobs WHERE "link" == \"{link}\"''', con)['like'][0]
    title = pd.read_sql(f'''SELECT title FROM new_jobs WHERE "link" == \"{link}\"''', con)['title'][0]
    predict = pd.read_sql(f'''SELECT predict FROM new_jobs WHERE "link" == \"{link}\"''', con)['predict'][0]
    sql_query = f'''INSERT INTO jobs (link, like, title, content, scraped, predict) VALUES (\"{link}\", \"{like}\", \"{title}\", \"{content}\", {scraped}, {predict})'''
    cursor.execute(sql_query)
    con.commit()
    sql_query =  f'''Delete FROM new_jobs where "link" == \"{link}\" '''
    cursor.execute(sql_query)
    con.commit()
    cursor.close()
    return new_job_page()

def make_table(jobs):
    df = pd.DataFrame()
    for job_1 in jobs:
        job = job_1.find('a').text
        job = job.strip()
        link = job_1.find('a')['href']
        url = 'https://www.indeed.ca' + link
        df = df.append({'title':job, 
                        'link':url,
                        'like': int(9),
                        'content': '',
                        'scraped': int(0),
                        'predict': int(0)
                        },ignore_index=True)
    return df


def return_jobs(html):
    soup = BeautifulSoup(html, 'lxml')
    links = soup.find_all('div', {"class":"title"})
    return links    

def process_page(url):
    html = return_html(url)
    jobs = return_jobs(html)
    df = make_table(jobs)
    try:
        np_link = next_page(html) 
    except: 
        np_link = False    
    return df, np_link

def next_page(html):
    soup = BeautifulSoup(html, 'lxml')
    link = soup.find_all(string=re.compile(r'^Next'))[0].previous_element.previous_element.previous_element['href']
    link = 'https://www.indeed.ca' + link
    return link

def make_daily_url(search_string = 'Data Scientist Toronto', location = 'Toronto', radius = '5'):
    location = location.replace(" ", "+")
    search_string = search_string.replace(" ", "+")
    url = 'https://www.indeed.ca/jobs?as_and=' + search_string + '&l=' + location \
        + '&radius=' + radius + '&sort=date&fromage=1'
    return url


def new_jobs(url):
    con = sqlite3.connect('jobs.db')
    df = process_page(url)[0]
    df.to_sql('new_jobs', con, if_exists='replace')
#    sql_query = f'''INSERT INTO jobs (link, like, title) VALUES (\"{link}\", 9, \"{title}\")'''

def add_new_job(link):
    con = sqlite3.connect('jobs.db')
    cursor = con.cursor()
    title = pd.read_sql(f'''SELECT title FROM new_jobs WHERE "link" == \"{link}\"''', con)['title'][0]
    sql_query = f'''INSERT INTO jobs (link, like, title) VALUES (\"{link}\", 9, \"{title}\")'''
    cursor.execute(sql_query)
    con.commit()
    cursor.close()

# @app.route('/predict_new_jobs', methods=['POST'])
# def predict_new_jobs():
#     args = flask.request.form
#     link = args.get('link')
#     con = sqlite3.connect('jobs.db')
#     tokens = tokens_from_link(link)
#     df = pd.DataFrame({'content': [tokens]})
#     pipe = dill.load(open('pipe.pkl', 'rb'))
#     answer_raw = pipe.predict_proba(df)[0][0]
#     answer = round(answer_raw *100, 2)
#     cursor = con.cursor()
#     sql_update_query =  f'''Update new_jobs set predict = \"{answer}\" where "link" == \"{link}\" '''
#     cursor.execute(sql_update_query)
#     con.commit()
#     cursor.close()
#     return new_job_page()

if __name__ == '__main__':
    app.run(port=5001, debug=True) 



# con = sqlite3.connect('jobs.db')
# cursor = con.cursor()
# sql_query = f'''INSERT INTO jobs VALUES (1,1,1,1) '''
# cursor.execute(sql_update_query)
# con.commit()
# cursor.close()

# pd.read_sql(f'''SELECT "link", "title" FROM new_jobs WHERE "link" == \"{link}\"''', con)
# link = pd.read_sql('''SELECT * FROM new_jobs''', con)['link'][0]
# pd.read_sql(f'''SELECT * FROM jobs ''', con)
# link
# pd.read_sql(f'''SELECT title FROM new_jobs WHERE "link" == \"{link}\"''', con)['title'][0]

# df.to_sql('new_jobs', con, if_exists='replace')


# url = make_daily_url()
# url
# df = process_page(url)[0]
# df


# def add_link(link, title):
# #    args = flask.request.form
# #    link = args.get('link')
#  #   title = args.get('title')
#     con = sqlite3.connect('jobs.db')
#     cursor = con.cursor()
#     sql_update_query =  f'''INSERT INTO jobs (link, like, title) VALUES (\"{link}\", 9, \"{title}\")'''
#     cursor.execute(sql_update_query)
#     con.commit()
#     cursor.close()
#     #return index()

# add_link('test2.com', 'test2')



# con = sqlite3.connect('jobs.db')


# cursor = con.cursor()

# sql_query = '''CREATE TABLE new_jobs ('title','link','like','content','scraped','predict') '''


# cursor.execute(sql_query)
# con.commit()
# cursor.close()
# pd.read_sql('''SELECT * FROM jobs where link == "http://www.google.com"''', con)
# pd.read_sql('''SELECT * FROM jobs where link == "test "''', con)['link'][0]


# con = sqlite3.connect('jobs.db')
# link_list = pd.read_sql(f"""SELECT link FROM new_jobs  where content == \'\'""", con)['link']
# for link in link_list:
#     cursor = con.cursor()
#     tokens = tokens_from_link(link) 
#     sql_update_query = f'''Update jobs set content = \'{tokens}\', scraped = 1 where link == \"{link}\"'''
#     cursor.execute(sql_update_query)
#     con.commit()
#     cursor.close()
# con.close()




# make_daily_url()

# https://www.indeed.ca/jobs?as_and=Data+Sciencist+Toronto&l=Toronto&radius=5&sort=date&fromage=1
# https://www.indeed.ca/jobs?as_and=Data+Scientist+Toronto&l=Toronto&fromage=1&limit=20&sort=date
# cursor = con.cursor()

# sql_update_query = """Update jobs set like = 2 where link == 'https://www.indeed.ca/pagead/clk?mo=r&ad=-6NYlbfkN0DwJAmpBVU0HWH4Z_N_8RfDOeMhFKYJiPRpnyg6LLHuPLkNgOo11i9V-bLs0ftKCWa4TLeb9bLmsFo2JjkdouuC257hV7LYNSwbGwfQFn2O2tF1k9H0i5RDHKRhto739PW9iZm9R4Iw9sPE6oygewO2FHMMJ4sG1wuGX2KChGefCJuWX0fD9hij0wAH03gvfaRoJxC6giahhsYJIyrRXQT9VluPkdvMhHOOZqnwAvHugBKCeJ-TJUsiGVNiS1_HtEOrABblrh4fTP4rgU1eIgXyEbR5smErwsBULXTk6Jw76YHZ-xGlP_SJRZPHPBcsbULrce3DaZIpHGQKugkX-S-LGf4LrylvWF1FUJa5Tlkmhzzq-xclS2g2TE07CXVdBlzVBICOi5uxAqvdLll1WlpxCTdn8vWfGmul4uz-Z03_8ccGp8rgXNErR9WslkJl6M8Zfl0mppFWdcXQwqkrinKVKfRHjHt0Gjnza9ja-Cql1GEPBIC9pIcH&p=1&fvj=0&vjs=3'"""
# sql_update_query = f"""Update jobs set like = 3 where link == \'{link}\'"""

# cursor.execute(sql_update_query)
# con.commit()
con = sqlite3.connect('jobs.db')
type(pd.read_sql('''SELECT * FROM jobs ''', con)['scraped'][0])
type(pd.read_sql('''SELECT * FROM jobs ''', con).iloc[461]['scraped'])
int(pd.read_sql('''SELECT * FROM new_jobs ''', con).iloc[0]['scraped'])
type(int(0))


# # pd.read_sql('''SELECT  like FROM jobs  where link == "https://www.indeed.ca/pagead/clk?mo=r&ad=-6NYlbfkN0DwJAmpBVU0HWH4Z_N_8RfDOeMhFKYJiPRpnyg6LLHuPLkNgOo11i9V-bLs0ftKCWa4TLeb9bLmsFo2JjkdouuC257hV7LYNSwbGwfQFn2O2tF1k9H0i5RDHKRhto739PW9iZm9R4Iw9sPE6oygewO2FHMMJ4sG1wuGX2KChGefCJuWX0fD9hij0wAH03gvfaRoJxC6giahhsYJIyrRXQT9VluPkdvMhHOOZqnwAvHugBKCeJ-TJUsiGVNiS1_HtEOrABblrh4fTP4rgU1eIgXyEbR5smErwsBULXTk6Jw76YHZ-xGlP_SJRZPHPBcsbULrce3DaZIpHGQKugkX-S-LGf4LrylvWF1FUJa5Tlkmhzzq-xclS2g2TE07CXVdBlzVBICOi5uxAqvdLll1WlpxCTdn8vWfGmul4uz-Z03_8ccGp8rgXNErR9WslkJl6M8Zfl0mppFWdcXQwqkrinKVKfRHjHt0Gjnza9ja-Cql1GEPBIC9pIcH&p=1&fvj=0&vjs=3"''', con)
# link = 'https://www.indeed.ca/pagead/clk?mo=r&ad=-6NYlbfkN0DwJAmpBVU0HWH4Z_N_8RfDOeMhFKYJiPRpnyg6LLHuPLkNgOo11i9V-bLs0ftKCWa4TLeb9bLmsFo2JjkdouuC257hV7LYNSwbGwfQFn2O2tF1k9H0i5RDHKRhto739PW9iZm9R4Iw9sPE6oygewO2FHMMJ4sG1wuGX2KChGefCJuWX0fD9hij0wAH03gvfaRoJxC6giahhsYJIyrRXQT9VluPkdvMhHOOZqnwAvHugBKCeJ-TJUsiGVNiS1_HtEOrABblrh4fTP4rgU1eIgXyEbR5smErwsBULXTk6Jw76YHZ-xGlP_SJRZPHPBcsbULrce3DaZIpHGQKugkX-S-LGf4LrylvWF1FUJa5Tlkmhzzq-xclS2g2TE07CXVdBlzVBICOi5uxAqvdLll1WlpxCTdn8vWfGmul4uz-Z03_8ccGp8rgXNErR9WslkJl6M8Zfl0mppFWdcXQwqkrinKVKfRHjHt0Gjnza9ja-Cql1GEPBIC9pIcH&p=1&fvj=0&vjs=3'

# def test1(link):
#     con = sqlite3.connect('jobs.db')
#     cursor = con.cursor()
#     sql_update_query = f"""Update jobs set like = 1 where link == \'{link}\'"""
#     cursor.execute(sql_update_query)
#     con.commit()
#     cursor.close()
#     return pd.read_sql('''SELECT * FROM jobs  ''', con)
# test1(link)

# con = sqlite3.connect('jobs.db')
# table = pd.read_sql('''
#     SELECT 
#     *
#     FROM jobs
#     ''', con)
# table.to_dict('dict')

# for i in table.link: print(i)
    



        # sqliteConnection = sqlite3.connect('SQLite_Python.db')
        # cursor = sqliteConnection.cursor()
        # print("Connected to SQLite")

        # sql_update_query = """Update SqliteDb_developers set salary = 10000 where id = 4"""
        # cursor.execute(sql_update_query)
        # sqliteConnection.commit()
        # print("Record Updated successfully ")
        # cursor.close()


# con = sqlite3.connect('jobs.db')
# cursor = con.cursor()
# sql_update_query = f"""Update jobs set link = 'www.test.com' where link == 'test ' """
# cursor.execute(sql_update_query)
# con.commit()
# cursor.close()        

# pd.read_sql('''SELECT * FROM jobs''', con)


# @app.route('/answer', methods=['POST'])
# def answer():
#     args = flask.request.form
#     link = args.get('link')
#     tokens = tokens_from_link(link)
#     df = pd.DataFrame({'content': [tokens]})
#     pipe = dill.load(open('pipe.pkl', 'rb'))
#     answer = pipe.predict_proba(df)[0][1]
#     return flask.render_template('answer.html', answer=answer)

# 

# link = 'https://ca.indeed.com/viewjob?jk=d1c3d577aed13ef1&tk=1e18fqogf54ar800&from=serp&vjs=3'
# new = content_from_link(link)
# df = pd.DataFrame({'job_string': [new]})
# pipe.predict_proba(df)[0][0]





# @app.route('/predict')
# def predict():
#     return flask.render_template('predict.html')


# @app.route('/test')
# def test():
#     return flask.render_template('upvote.html')
