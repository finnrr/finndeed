

import pandas as pd
import time


import nltk
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from nltk.tokenize import RegexpTokenizer
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline 
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn_pandas import DataFrameMapper, FunctionTransformer
nltk.download('stopwords')
# cvec = CountVectorizer(stop_words='english')

def title_to_tokens(title):
    lower_case = title.lower()
    retokenizer = RegexpTokenizer(r'\w+')
    words = retokenizer.tokenize(lower_case)
    stops = set(stopwords.words('english'))
    meaningful_words = [w for w in words if not w in stops]
    stemmer = PorterStemmer()
    stemmed_words = [stemmer.stem(w) for w in meaningful_words]
    return " ".join(stemmed_words) 

# df = pd.DataFrame()
# df['like'] = 0
# type(df)
# title_to_tokens(job_string[0])

# df = pd.DataFrame(job_string, columns=['job_string'])
# df['like'] = 0

# d = pd.DataFrame(job_string_2, columns=['job_string'])
# d['like'] = 1
# df = df.append(d)
# df

# df
# mapper = DataFrameMapper([
#     ('job_string', [FunctionTransformer(title_to_tokens), CountVectorizer()])
# ], df_out=True)

# model = LogisticRegression()

# pipe = make_pipeline(mapper, model)
# X = df.drop('like', axis=1)
# y = df['like']
# pipe.fit(X , y)


# new = pd.DataFrame({'job_string': [x]})

# pipe.predict_proba(new)









# soup.find_all('div', {"class":"title"})[0].find('a').text

# a
# html = urllib.request.urlopen('https://www.indeed.ca/viewjob?jk=43ebb0c478e463bf&tk=1e100je3n54as800&from=serp&vjs=3').read()
# print(text_from_html(html))




###----------------------------
from bs4 import BeautifulSoup
from bs4.element import Comment
import pandas as pd
from selenium import webdriver
from selenium.webdriver import Firefox
from selenium.webdriver.firefox.options import Options
import urllib.request
import time
import random
import re 
import sqlite3

# make url
def make_url(search_string = 'Data Science', location = 'Toronto', radius = '5'):
    location = location.replace(" ", "+")
    search_string = search_string.replace(" ", "+")
    url = 'https://www.indeed.ca/jobs?q=' + search_string + '&l=' + location \
        + '&radius=' + radius + '&sort=date'
    return url

# url = make_url('Data Science', 'Toronto', '5')

# fake out browser
def return_html(url):
    options = Options()
    options.headless = True
    browser = Firefox(executable_path='geckodriver', options=options)
    browser.get(url)
    return browser.page_source

# html = return_html(url)

# get list of jobs
def return_jobs(html):
    soup = BeautifulSoup(html, 'lxml')
    links = soup.find_all('div', {"class":"title"})
    return links

# jobs = return_jobs(html)

# turn data into pandas table
def make_table(jobs):
    df = pd.DataFrame()
    for job_1 in jobs:
        job = job_1.find('a').text
        job = job.strip()
        link = job_1.find('a')['href']
        url = 'https://www.indeed.ca' + link
        try:
            d = player(player)
        except:
            pass
        df = df.append({'title':job, 
                        'link':url,
                        'like': int(0),
                        'content': ''
                        },ignore_index=True)
    return df

# df = make_table(jobs)

# job words from job link
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

def content_from_link(link):
    html = return_html(link)
    return text_from_html(html)

# populate table with content
def cfl_table(df):
    '''content from link across table'''
    for (i, r) in df.iterrows():
        print(df['link'][i]) 
        df['content'][i] = content_from_link(df['link'][i])
        print(df['content'][i])
        time.sleep(random.randint(1, 20) / 10)
    return df  

# return next page link
def next_page(html):
    soup = BeautifulSoup(html, 'lxml')
    link = soup.find_all(string=re.compile(r'^Next'))[0].previous_element.previous_element.previous_element['href']
    link = 'https://www.indeed.ca' + link
    return link

# return df of job links from url
def process_page(url):
    html = return_html(url)
    jobs = return_jobs(html)
    df = make_table(jobs)
    try:
        np_link = next_page(html) 
    except: 
        np_link = False    
    return df, np_link

# get all links from all pages, WARNING: slow
def get_full_dataframe(url, stop_limit=10):
    df = pd.DataFrame()
    itterate = True
    while (itterate != False) and (stop_limit > 0):
        print(f'processing {url}')
        result = process_page(url)
        itterate = result[1]
        stop_limit -= 1
        url = result[1]
        df = df.append(result[0], ignore_index=True)
    return df


# con = sqlite3.connect('jobs.db')
# df = pd.read_sql('''SELECT * FROM jobs''', con)
# df


# def test1(link):
#     con = sqlite3.connect('jobs.db')
#     cursor = con.cursor()
#     sql_update_query = f"""Update jobs set like = 1 where link == \'{link}\'"""
#     cursor.execute(sql_update_query)
#     con.commit()
#     cursor.close()
#     return pd.read_sql('''SELECT * FROM jobs  ''', con)
# test1(link)


# link = 'https://www.indeed.ca/pagead/clk?mo=r&ad=-6NYlbfkN0DwJAmpBVU0HWH4Z_N_8RfDOeMhFKYJiPRpnyg6LLHuPLkNgOo11i9V-bLs0ftKCWa4TLeb9bLmsFo2JjkdouuC257hV7LYNSwbGwfQFn2O2tF1k9H0i5RDHKRhto739PW9iZm9R4Iw9sPE6oygewO2FHMMJ4sG1wuGX2KChGefCJuWX0fD9hij0wAH03gvfaRoJxC6giahhsYJIyrRXQT9VluPkdvMhHOOZqnwAvHugBKCeJ-TJUsiGVNiS1_HtEOrABblrh4fTP4rgU1eIgXyEbR5smErwsBULXTk6Jw76YHZ-xGlP_SJRZPHPBcsbULrce3DaZIpHGQKugkX-S-LGf4LrylvWF1FUJa5Tlkmhzzq-xclS2g2TE07CXVdBlzVBICOi5uxAqvdLll1WlpxCTdn8vWfGmul4uz-Z03_8ccGp8rgXNErR9WslkJl6M8Zfl0mppFWdcXQwqkrinKVKfRHjHt0Gjnza9ja-Cql1GEPBIC9pIcH&p=1&fvj=0&vjs=3'


# words = 
def fill_sql_content(link):
    con = sqlite3.connect('jobs.db')
    cursor = con.cursor()
    words = content_from_link(link)
    tokens = title_to_tokens(words)
   # print(tokens)
    sql_update_query = f"""Update jobs set content = \'{tokens}\' where link == \"{link}\""""
    #print(sql_update_query)
    cursor.execute(sql_update_query)
    con.commit()
    cursor.close()
    print( pd.read_sql(f"""SELECT * FROM jobs  where link == \'{link}\'""", con) )
    con.close()
    #print(link)

con = sqlite3.connect('jobs.db')
link_list = pd.read_sql(f"""SELECT link FROM jobs  where content == \'\'""", con)['link']
con.close()
for i in link_list:
    fill_sql_content(i)
con = sqlite3.connect('jobs.db')
df = pd.read_sql(f"""SELECT * FROM jobs """, con)
df.to_csv('jobs_all_full.csv')



# pd.read_sql('''SELECT * FROM jobs''', con)
# pd.read_sql(f"""SELECT link FROM jobs  where content == \'\'""", con).values





# fill_sql_content(link)
# f""" some '''{test} stuff''' sdomf """

# df = pd.read_csv('460_jobs.csv')
# df2 = cfl_table(df)

# dfx.to_csv('460_jobs.csv')


# con = sqlite3.connect('jobs.db')

# dfx.to_sql('jobs', con, if_exists='append')



# content = ''' Find jobs Company reviews Find salaries Upload your resume Sign in | En Fr | Employers / Post Job   what job title, keywords, or company where city or province Find jobs Advanced Job Search Manager - Data and Analytics ISG Search Inc 10 reviews ISG Search Inc 10 reviews Read what people are saying about working here. Apply Now Apply Now Save this job Toronto, ON Permanent $140,000 a year Our client is a market leader in the financial services and security space. They develop internal and external customer facing data driven solutions using the latest in-memory technologies and analytics tools covering data warehousing, reporting, analytics, machine learning and data integrations.  This group runs it's own development and production environments and manage all the responsibilities that come along with this.  We're looking for self-starter, innovator, problem solver... some who possesses excellent leadership, communication and collaboration skills.  Ideally you come from a development background and have moved into leadership where you've built and scaled high functioning teams covering multiple disciplines, environments and processes.  Your Challenge:   Oversee and lead the development and operations of key enterprise data systems, analytics and reporting environments Work hand in hand with internal stakeholders and customers in gathering requirements and implementing best in class solutions Define and manage multiple projects, timelines and resources Provide guidance and direction on technology and implementation choices Maintain and enhance operations and availability for key systems Build and maintain data integrations between cloud and on premises solutions Ensure data quality and correctness Participate and lead proof of concept designs and implementations Continually innovate to add value, optimize processes, tools and technologies Balance and grow the team to meet current and future demands Required Skills and Experience:   Bachelor's degree or higher in Computer Science, Engineering or related technology field 3+ years managing teams of software developers and data engineers 5+ years using agile methodologies 5+ years of experience building data driven solutions 5+ years of OLTP and OLAP experience (SAP HANA, DB2, SQL SERVER …) 5+ years working with Analytic and Reporting tools (BOBJ, SAS, Tableau …) 5+ years working in operational environments 2+ years of experience with Salesforce, NetSuite and advanced rating platforms 2+ years in business and data analysis Working knowledge of machine learning tools, processes and technologies ISG Search Inc - 2 days ago - save job report job - original job Analytics Manager jobs in Toronto, ON Jobs at ISG Search in Toronto, ON Analytics Manager salaries in Toronto, ON Company Info Follow Get job updates from ISG Search Inc. By selecting Follow, you agree to get updated information and new jobs for this company by email. You can cancel alerts at anytime. ISG Search Inc 10 reviews isgSearch is a leading Canadian recruitment firm based in the GTA specializing in Information Technology. We understand how the IT indust... Hiring Lab Browse Jobs Browse Companies Salaries Work at Indeed About Help Centre © 2020 Indeed Privacy Centre Cookies, Privacy and Terms Let Employers Find You Upload Your Resume'''




# dfx = get_full_dataframe(url)
# dfx
#     process_page(url)

# url = make_url('Data Science', 'Toronto', '5')

# process_page(url)

# while next_page(html)


# try:
#     url =  + next_page(html)


# pages= soup.find_all('div', {"class":"pagination"})
# html






















# job_string = ['''Senior Data Consultant

# On behalf of our client in the Telco Sector, PROCOM is looking for a Senior Data Consultant.

# Senior Data Consultant – Job Description

#     The candidate will be responsible for enabling advanced analytics and machine learning solutions through data engineering and data science.
#     Contribute to the development of Next Best Action and Customer Journey Analytics by engineering data assets that drive near real-time predictive models and trigger-based strategies leveraging SAS, SPARK, Python, etc.
#     Act as the bridge between IT Data Engineers/Developers and Marketing Data Scientists to translate raw data tables into actionable views that can be leveraged for machine learning applications.
#     Be the Data Steward to maintain / enhance data assets and help end users resolve data questions.
#     Work collaboratively with business partners teams to understand, compartmentalize complex and ambiguous business issues into crisp and manageable pieces
#     Own the end-to-end analysis including pre-analysis, trigger detection, predictive modeling, post campaign analysis.
#     Present customer insights to cross functional business partners including Marketing, Channel, Products & Services, Customer Experience and Digital.
#     Maintain a solid and updated knowledge in the predictive modeling, data mining field including applications of Big Data, Text Analytics, Social Network Analytics, Cross Channel Modelling

# Senior Data Consultant – Mandatory Skills

# Technical Expertise

#     Advanced Knowledge in various advanced predictive modeling/data mining techniques to ensure effective solutions to marketing issues
#     Extensive Hands-on experience in SAS, SPARK, Python, SQL and other data query language on UNIX / PC and comfortable working with very large data sets and databases
#     Strong Quantitative, analytical and problem-solving skills

# Business Orientation

# Advanced Business acumen, relationship management and influencing without authority skills

# Ability to provide actionable recommendations based on data analysis

# Strong Ability to prioritize tasks from multiple stakeholders and meet deadlines with quality deliverables

# Self-starter and able to operate with minimal supervision

# Advanced Written, oral, communications, presentation skills and knowledge of Microsoft office/Google suite

# Senior Data Consultant – Nice to Have Skills

#     Fundamental understanding of banking as well as knowledge of products and services
#     Marketing Background
#     Event Planning College Certificate considered an asset

# Senior Data Consultant - Assignment Start Date

# ASAP – 12 months to start

# Senior Data Consultant - Assignment Location

# Downtown Toronto''']


# job_string_2 = ['''
# What is the opportunity?

# As a Full-Stack Data Scientist, you will analyze, design, and implement AIOps solutions at RBC Technology Infrastructure (TI). Leveraging leading edge technologies and various data sets, you will apply machine learning and statistical modelling techniques to facilitate informed decision-making and business process optimization. Moreover, designing and implementing end-to-end machine learning products will be part of your journey.


# What will you do?

#     Work on challenging and research-based initiatives using advanced machine learning methods focusing on tangible outcomes
#     Provide analytics support to all TI pillars. This involves collaborating proactively with various business and technical units to identify business opportunities and designing innovative solutions to optimize processes and promote informed decision-making.
#     Prepare and integrate large and various types of data (structured/non-structured)
#     Implement machine learning models, data mining methods, and statistical analysis
#     Leverage visualization tools/packages to create powerful representations of results
#     Produce data-driven insights to help in informed decisions and actions by telling a convincing story
#     Effectively communicate findings to business partners and executives
#     Collaborate with the development team to deploy production-scale solutions
#     Quickly learn new tools and technologies and use them in the daily analytics exercises


# What do you need to succeed?

# Must-have

#     Bachelor, Masters or PhD. in Computer Science, Statistics, or relevant fields.
#     Expert in Python programming to write production-ready codes
#     Strong data profiling, cleaning, mining and technical documentation skills
#     2+ years of experience in building machine learning models (Supervised/Unsupervised)
#     2+ year(s) experience with NLP and text analytics methods and packages
#     Experience building end-to-end pipeline and deploying machine learning models
#     Experience with big data technologies – parallel processing techniques and Apache Spark, Hadoop ecosystem, NoSQL/SQL databases
#     Experience with container-type environment: Docker, Kubernetes, Openshift, PCF
#     Experience with custom Web interfaces, API calls, and systems integration
#     Familiar with Linux environment, shell scripting, and Git
#     Experience working in an agile environment


# Nice-to-have

#     Software engineering background with a focus on statistics and/or analytics
#     Experience in deep learning methods for NLP in Tensorflow or Pytorch
#     Familiar with Technology Infrastructure
# ''']

# job_string_3 = ['''
# SENIOR COMMERCIAL CREDIT AND LENDING DATA ANALYST

# LOCATION: TORONTO

# Let’s Talk About You

# You want to Own Your Career. You’re serious about rising as far and as fast as your work and achievements can take you. And you’re ready to write the next chapter of your career story: a challenging and rewarding role as a Senior Data Analyst.

# A Word About Us


# Capco – Forming the Future of Finance

# Capco is a distinctly and positively different place to work. Much more than consultants, we are active participants in the global financial services industry. Our passionate business and technology professionals enjoy a unique environment where they are actively encouraged to apply intellect, innovation, experience and teamwork. We are dedicated to fully supporting our world class clients as they respond to challenges and opportunities in: Banking, Capital Markets, Finance Risk & Compliance, Insurance and Wealth and Investment Management. Experience Capco for yourself at capco.com.

# Let’s Get Down to Business

# Capco is looking for talented, innovative and creative people to join our emerging data analytics team to work on a number of projects and applications.

# Responsibilities

#     Define and obtain source data required to successfully deliver insights and use cases
#     Determine the data mapping required to join multiple data sets together across multiple sources
#     Create methods to highlight and report data inconsistencies, allowing users to review and provide feedback
#     Propose suitable data migration sets to relevant stakeholders
#     Assist teams with processing, planning, tracking, scope and coordination of data migration sets as required
#     Work on end-to-end flow of a broad range of analytics use cases
#     Maintain RAID logs for projects, and ensure that these are fed into the programme effectively
#     Lead and work closely with all stakeholders creating a strong culture of transparency and collaboration
#     Adhere strictly to compliance/operational risk controls and due diligence in accordance with regulatory standards, policies and practices
#     Report concerns or observations in terms of control weaknesses, compliance breaches and operational risk
#     Maintains control standards, including timely implementation of internal and external audit points

# Must Haves

#     Demonstrate a continual desire to implement “strategic” or “optimal” solutions and where possible, avoid workarounds or short-term tactical solutions
#     Understand all changes in the wider context of the business lines/areas
#     Manage stakeholder expectations and ensure that robust communication and escalation mechanisms are in place across project portfolios
#     Possess experience using data models and data dictionaries in a corporate or large commercial credit and lending context.
#     Knowledge of one or more of the following domains:
#         Large commercial / corporate loan booking systems and data
#         Risk rating of assets,Benchmark rates (e.g. LIBOR replacement)


# Technical capabilities

#     Ability to write SQL queries and Pyspark. Scala knowledge preferable and navigate data bases especially Hive, CMD, Putty, Note++
#     Good knowledge of SDLC and formal Agile processes, with a bias towards TDD and a willingness to test products as part of the delivery cycle
#     Experience using and flattening XML
#     Experience with big data programmes

# Nice to Haves


# We are also looking for individuals who demonstrate the following traits:

#     A good understanding of control requirements surrounding data handling
#     Ability to assess operational risks during analysis, implementation, planning and execution phases in conjunction with stakeholders
#     Knowledge and experience in data quality & governance


# Professional experience is important. But it’s paramount you share our belief in disruptive innovation that puts clients ahead in a tough market. From Day One, your key skill will be to perceive new and better ways of doing things to give your clients an unfair advantage.


# Now Take the Next Step

# If you’re looking forward to progressing your career with us, then we’re looking forward to receiving your application.
# ''']
