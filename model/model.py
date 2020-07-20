job_string = ['''Senior Data Consultant

On behalf of our client in the Telco Sector, PROCOM is looking for a Senior Data Consultant.

Senior Data Consultant – Job Description

    The candidate will be responsible for enabling advanced analytics and machine learning solutions through data engineering and data science.
    Contribute to the development of Next Best Action and Customer Journey Analytics by engineering data assets that drive near real-time predictive models and trigger-based strategies leveraging SAS, SPARK, Python, etc.
    Act as the bridge between IT Data Engineers/Developers and Marketing Data Scientists to translate raw data tables into actionable views that can be leveraged for machine learning applications.
    Be the Data Steward to maintain / enhance data assets and help end users resolve data questions.
    Work collaboratively with business partners teams to understand, compartmentalize complex and ambiguous business issues into crisp and manageable pieces
    Own the end-to-end analysis including pre-analysis, trigger detection, predictive modeling, post campaign analysis.
    Present customer insights to cross functional business partners including Marketing, Channel, Products & Services, Customer Experience and Digital.
    Maintain a solid and updated knowledge in the predictive modeling, data mining field including applications of Big Data, Text Analytics, Social Network Analytics, Cross Channel Modelling

Senior Data Consultant – Mandatory Skills

Technical Expertise

    Advanced Knowledge in various advanced predictive modeling/data mining techniques to ensure effective solutions to marketing issues
    Extensive Hands-on experience in SAS, SPARK, Python, SQL and other data query language on UNIX / PC and comfortable working with very large data sets and databases
    Strong Quantitative, analytical and problem-solving skills

Business Orientation

Advanced Business acumen, relationship management and influencing without authority skills

Ability to provide actionable recommendations based on data analysis

Strong Ability to prioritize tasks from multiple stakeholders and meet deadlines with quality deliverables

Self-starter and able to operate with minimal supervision

Advanced Written, oral, communications, presentation skills and knowledge of Microsoft office/Google suite

Senior Data Consultant – Nice to Have Skills

    Fundamental understanding of banking as well as knowledge of products and services
    Marketing Background
    Event Planning College Certificate considered an asset

Senior Data Consultant - Assignment Start Date

ASAP – 12 months to start

Senior Data Consultant - Assignment Location

Downtown Toronto''']


job_string_2 = ['''
What is the opportunity?

As a Full-Stack Data Scientist, you will analyze, design, and implement AIOps solutions at RBC Technology Infrastructure (TI). Leveraging leading edge technologies and various data sets, you will apply machine learning and statistical modelling techniques to facilitate informed decision-making and business process optimization. Moreover, designing and implementing end-to-end machine learning products will be part of your journey.


What will you do?

    Work on challenging and research-based initiatives using advanced machine learning methods focusing on tangible outcomes
    Provide analytics support to all TI pillars. This involves collaborating proactively with various business and technical units to identify business opportunities and designing innovative solutions to optimize processes and promote informed decision-making.
    Prepare and integrate large and various types of data (structured/non-structured)
    Implement machine learning models, data mining methods, and statistical analysis
    Leverage visualization tools/packages to create powerful representations of results
    Produce data-driven insights to help in informed decisions and actions by telling a convincing story
    Effectively communicate findings to business partners and executives
    Collaborate with the development team to deploy production-scale solutions
    Quickly learn new tools and technologies and use them in the daily analytics exercises


What do you need to succeed?

Must-have

    Bachelor, Masters or PhD. in Computer Science, Statistics, or relevant fields.
    Expert in Python programming to write production-ready codes
    Strong data profiling, cleaning, mining and technical documentation skills
    2+ years of experience in building machine learning models (Supervised/Unsupervised)
    2+ year(s) experience with NLP and text analytics methods and packages
    Experience building end-to-end pipeline and deploying machine learning models
    Experience with big data technologies – parallel processing techniques and Apache Spark, Hadoop ecosystem, NoSQL/SQL databases
    Experience with container-type environment: Docker, Kubernetes, Openshift, PCF
    Experience with custom Web interfaces, API calls, and systems integration
    Familiar with Linux environment, shell scripting, and Git
    Experience working in an agile environment


Nice-to-have

    Software engineering background with a focus on statistics and/or analytics
    Experience in deep learning methods for NLP in Tensorflow or Pytorch
    Familiar with Technology Infrastructure
''']

job_string_3 = ['''
SENIOR COMMERCIAL CREDIT AND LENDING DATA ANALYST

LOCATION: TORONTO

Let’s Talk About You

You want to Own Your Career. You’re serious about rising as far and as fast as your work and achievements can take you. And you’re ready to write the next chapter of your career story: a challenging and rewarding role as a Senior Data Analyst.

A Word About Us


Capco – Forming the Future of Finance

Capco is a distinctly and positively different place to work. Much more than consultants, we are active participants in the global financial services industry. Our passionate business and technology professionals enjoy a unique environment where they are actively encouraged to apply intellect, innovation, experience and teamwork. We are dedicated to fully supporting our world class clients as they respond to challenges and opportunities in: Banking, Capital Markets, Finance Risk & Compliance, Insurance and Wealth and Investment Management. Experience Capco for yourself at capco.com.

Let’s Get Down to Business

Capco is looking for talented, innovative and creative people to join our emerging data analytics team to work on a number of projects and applications.

Responsibilities

    Define and obtain source data required to successfully deliver insights and use cases
    Determine the data mapping required to join multiple data sets together across multiple sources
    Create methods to highlight and report data inconsistencies, allowing users to review and provide feedback
    Propose suitable data migration sets to relevant stakeholders
    Assist teams with processing, planning, tracking, scope and coordination of data migration sets as required
    Work on end-to-end flow of a broad range of analytics use cases
    Maintain RAID logs for projects, and ensure that these are fed into the programme effectively
    Lead and work closely with all stakeholders creating a strong culture of transparency and collaboration
    Adhere strictly to compliance/operational risk controls and due diligence in accordance with regulatory standards, policies and practices
    Report concerns or observations in terms of control weaknesses, compliance breaches and operational risk
    Maintains control standards, including timely implementation of internal and external audit points

Must Haves

    Demonstrate a continual desire to implement “strategic” or “optimal” solutions and where possible, avoid workarounds or short-term tactical solutions
    Understand all changes in the wider context of the business lines/areas
    Manage stakeholder expectations and ensure that robust communication and escalation mechanisms are in place across project portfolios
    Possess experience using data models and data dictionaries in a corporate or large commercial credit and lending context.
    Knowledge of one or more of the following domains:
        Large commercial / corporate loan booking systems and data
        Risk rating of assets,Benchmark rates (e.g. LIBOR replacement)


Technical capabilities

    Ability to write SQL queries and Pyspark. Scala knowledge preferable and navigate data bases especially Hive, CMD, Putty, Note++
    Good knowledge of SDLC and formal Agile processes, with a bias towards TDD and a willingness to test products as part of the delivery cycle
    Experience using and flattening XML
    Experience with big data programmes

Nice to Haves


We are also looking for individuals who demonstrate the following traits:

    A good understanding of control requirements surrounding data handling
    Ability to assess operational risks during analysis, implementation, planning and execution phases in conjunction with stakeholders
    Knowledge and experience in data quality & governance


Professional experience is important. But it’s paramount you share our belief in disruptive innovation that puts clients ahead in a tough market. From Day One, your key skill will be to perceive new and better ways of doing things to give your clients an unfair advantage.


Now Take the Next Step

If you’re looking forward to progressing your career with us, then we’re looking forward to receiving your application.
''']


import pandas as pd
import time
import sqlite3
import dill
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
from sklearn.pipeline import Pipeline


# nltk.download('stopwords')
# # cvec = CountVectorizer(stop_words='english')

# def title_to_tokens(title):
#     lower_case = title.lower()
#     retokenizer = RegexpTokenizer(r'\w+')
#     words = retokenizer.tokenize(lower_case)
#     stops = set(stopwords.words('english'))
#     meaningful_words = [w for w in words if not w in stops]
#     stemmer = PorterStemmer()
#     stemmed_words = [stemmer.stem(w) for w in meaningful_words]
#     return " ".join(stemmed_words) 

#create dummy df
# df = pd.DataFrame(job_string, columns=['job_string'])
# df['like'] = 0

# d = pd.DataFrame(job_string_2, columns=['job_string'])
# d['like'] = 1
# df = df.append(d)
# df

#make mapper
# mapper = DataFrameMapper([
#     ('job_string', [FunctionTransformer(title_to_tokens), CountVectorizer()])
# ], df_out=True)

#make model
model = LogisticRegression()

#make pipe
pipe = make_pipeline(mapper, model)

#fit pipe with dummy
X = df.drop('like', axis=1)
y = df['like']
pipe.fit(X , y)

#predict dummy
# new = pd.DataFrame({'job_string': [x]})
# pipe.predict_proba(new)

#export pipe pickle
dill.dump(pipe, open("pipe.pkl", "wb"))






###--------------


def retrain():
    con = sqlite3.connect('../jobs.db')
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

pd.read_sql('''SELECT * FROM jobs''', con)