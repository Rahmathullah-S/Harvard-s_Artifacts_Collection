import streamlit as st
import requests
import pandas as pd
import mysql.connector
#-------------------------------------------------
api_key="f3bd7bbd-0987-4dd9-bae8-f0aab5856547"
url2 = "https://api.harvardartmuseums.org/object"
sql_password='password'
database_name='test11'
#--------------------------------------------------

final=[]
def get_final_out(artifact):
    object_json2=[]
    for i in range(1,26):
        params = {'apikey':api_key,
              "size":100,
              "page":i,
              'classification':artifact}

        response2 = requests.get(url2,params)
        print(i)
        object_details2=response2.json()
        object_json2.extend(object_details2['records'])

    final.clear()
    final.append(object_json2)
    return object_json2

def metadata_f(final_out):
    metadata=[]
    for j in range(2500):
            temp=dict(id=final_out[j]['id'],
            title=final_out[j]['title'],
            culture=final_out[j]['culture'],
            period=final_out[j]['period'],
            century=final_out[j]['century'],
            medium=final_out[j]['medium'],
            dimensions=final_out[j]['dimensions'],
            description=final_out[j]['description'],
            department=final_out[j]['department'],
            classification=final_out[j]['classification'],
            accessionyear=final_out[j]['accessionyear'],
            accessionmethod=final_out[j]['accessionmethod'])
            for k,v in temp.items():
                if v is None:
                    temp[k]=0
            metadata.append(temp)
    return metadata

def media_f(final_out):
    media=[]
    for j in range(2500):
        temp=dict(
        objectid=final_out[j]['objectid'],
        imagecount=final_out[j]['imagecount'],
        mediacount=final_out[j]['mediacount'],
        colorcount=final_out[j]['colorcount'],
        rank=final_out[j]['rank'],
        datebegin=final_out[j]['datebegin'],
        dateend=final_out[j]['dateend'])
        for k,v in temp.items():
            if v is None:
                temp[k]=0
        media.append(temp)
    return media

def color_f(final_out):
    colr=[]
    for j in range(2500):
        f=final_out[j].get('colors')
        if f is not None:
            for k in f:
                temp=dict(
                objectid=final_out[j]['objectid'],
                color=k.get('color'),
                spectrum=k.get('spectrum'),
                hue=k.get('hue'),
                percent=k.get('percent'),
                css3=k.get('css3'))
                for k,v in temp.items():
                    if v is None:
                        temp[k]=0
                colr.append(temp)
    return colr


#-----------------------------------------------------SQL portion----------------
def connection_f():
    connection = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password=sql_password,
        database=database_name)

    c= connection.cursor()
    
    return c,connection


def metadata_dff(metadata):
    c,connection=connection_f()
    
    c.execute('''CREATE TABLE IF NOT EXISTS metadata(ID int not null primary key, Title varchar(500),Culture varchar(30),
        Period varchar(80), Century varchar(30), Medium varchar(500),Dimensions varchar(400), Description varchar(2000),
        Department varchar(100),Classification varchar(50), Accession_Year int, Accession_Method varchar(30));''')

    for i in metadata:
        values=(i['id'],i['title'],i['culture'],i['period'],i['century'],i['medium'],i['dimensions'],i['description'],i['department'],i['classification'],i['accessionyear'],i['accessionmethod'])
        c.execute('''insert into metadata(ID,Title,Culture,Period,Century,Medium,Dimensions,Description,Department,Classification,
        Accession_Year,Accession_Method) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',values)

    connection.commit()

    c.execute('select *from metadata')
    rows = c.fetchall()

    columns = [i[0] for i in c.description]
    df_metadata = pd.DataFrame(rows,columns=columns)
    c.close()
    connection.close()
    return df_metadata



def media_dff(media):
    c,connection=connection_f()
    
    c.execute('''CREATE TABLE IF NOT EXISTS media_table(Object_ID int not null, Image_Count int, Media_Count int,Color_Count int,Rank_ int, Date_Begin int,Date_End int,foreign key (Object_ID) references metadata(ID));''')

    for i in media:
        values=(i['objectid'],i['imagecount'],i['mediacount'],i['colorcount'],i['rank'],i['datebegin'],i['dateend'])
        c.execute('''insert into media_table(Object_ID,Image_Count,Media_Count,Color_Count,Rank_,Date_Begin,Date_End) VALUES(%s,%s,%s,%s,%s,%s,%s)''',values)

    connection.commit()

    c.execute('select *from media_table')
    rows = c.fetchall()

    columns = [i[0] for i in c.description]
    df_media= pd.DataFrame(rows,columns=columns)
    c.close()
    connection.close()

    return df_media

def color_dff(color):
    c,connection=connection_f()
    
    c.execute('''CREATE TABLE IF NOT EXISTS color_table(Object_ID int not null, Color varchar(20), Spectrum varchar(20),Hue varchar(20),Percent REAL, CSS3 varchar(20),foreign key (Object_ID) references metadata(ID));''')

    for i in color:
        values=(i['objectid'],i['color'],i['spectrum'],i['hue'],i['percent'],i['css3'])
        c.execute('''insert into color_table(Object_ID,Color,Spectrum,Hue,Percent,CSS3) VALUES(%s,%s,%s,%s,%s,%s)''',values)
    connection.commit()

    c.execute('select *from color_table')
    rows = c.fetchall()

    columns = [i[0] for i in c.description]
    df_color= pd.DataFrame(rows,columns=columns)
    c.close()
    connection.close()

    return df_color
#-------------------------------------------------------------
def questions():
    questions = ['--None--',
                 '1. List all artifacts from the 11th century belonging to Byzantine culture.',
                 '2. What are the unique cultures represented in the artifacts?',
                 '3. List all artifacts from the Archaic Period.',
                 '4. List artifact titles ordered by accession year in descending order.',
                 '5. How many artifacts are there per department?',
                 '6. Which artifacts have more than 3 images?',
                 '7. What is the average rank of all artifacts?',
                 '8. Which artifacts have a higher mediacount than color count?',
                 '9. List all artifacts created between 1500 and 1600.',
                 '10. How many artifacts have no media files?',
                 '11. What are all the distinct hues used in the dataset?',
                 '12. What are the top 5 most used colors by frequency?',
                 '13. What is the average coverage percentage for each hue?',
                 '14. List all colors used for a given artifact ID.',
                 '15. What is the total number of color entries in the dataset?',
                 '16. List artifact titles and hues for all artifacts belonging to the Byzantine culture.',
                 '17. List each artifact title with its associated hues.',
                 '18. Get artifact titles, cultures, and media ranks where the period is not null.',
                 '19. Find artifact titles ranked in the top 10 that include the color hue "Grey".',
                 '20. How many artifacts exist per classification, and what is the average media count for each?',
                 '21. List the artifact Title and ID for artifacts that were accessioned after December 1999, and whose accession method is either _Purchase_ or _Partially Gift/Partially Purchase',
                 '22. List the artifacts with their ID, title, and rank in ascending order of rank.',
                 '23. How many artifacts have no culture?',
                 '24. List the artifact ID and title for artifacts that do not have media count, color count, or image count.',
                 '25. Which Hue is least used in artifacts?',
                 '26.List the artifact ID and title for artifacts that do not have a medium count.']
    return questions

def q1(c):
    c.execute(''' select * from metadata where Century='20th century' and Culture = 'German'  ''')

    rows = c.fetchall()
    columns = [i[0] for i in c.description]
    query_df= pd.DataFrame(rows,columns=columns)
    #st.dataframe(query_df)
    return query_df
 
def q2(c):
    c.execute('select distinct(Culture) from metadata')

    rows = c.fetchall()
    columns = [i[0] for i in c.description]
    query_df= pd.DataFrame(rows,columns=columns)
    return query_df

def q3(c):  
    c.execute(''' select * from metadata where Period='Archaic period' ''')

    rows = c.fetchall()
    columns = [i[0] for i in c.description]
    query_df= pd.DataFrame(rows,columns=columns)
    return query_df

def q4(c):  
    c.execute('select Title, Accession_Year from metadata order by Accession_Year desc ')

    rows = c.fetchall()
    columns = [i[0] for i in c.description]
    query_df= pd.DataFrame(rows,columns=columns)
    return query_df
def q5(c):  
    c.execute('select Department,Count(Department) from metadata group by Department')

    rows = c.fetchall()
    columns = [i[0] for i in c.description]
    query_df= pd.DataFrame(rows,columns=columns)
    return query_df
def q6(c):  
    c.execute('select Title,Object_ID,Image_Count from metadata inner join media_table on metadata.ID=media_table.Object_ID where Image_Count>3')

    rows = c.fetchall()
    columns = [i[0] for i in c.description]
    query_df= pd.DataFrame(rows,columns=columns)
    return query_df
def q7(c):  
    c.execute('select AVG(Rank_) from media_table')

    rows = c.fetchall()
    columns = [i[0] for i in c.description]
    query_df= pd.DataFrame(rows,columns=columns)
    return query_df
def q8(c):  
    c.execute('''select Title,Object_ID,Media_Count,Color_Count from metadata inner join media_table on metadata.ID=media_table.Object_ID where Media_Count > Color_Count''')
    
    rows = c.fetchall()
    columns = [i[0] for i in c.description]
    query_df= pd.DataFrame(rows,columns=columns)
    return query_df
def q9(c):  
    c.execute('''select Title,Object_ID,Date_Begin,Date_End from metadata inner join media_table on metadata.ID=media_table.Object_ID where (Date_Begin>1499 AND Date_End<1601)''')

    rows = c.fetchall()
    columns = [i[0] for i in c.description]
    query_df= pd.DataFrame(rows,columns=columns)
    return query_df
def q10(c):  
    c.execute(''' select Count(Media_Count) as Media_Count from media_table where Media_Count=0 ''')
    

    rows = c.fetchall()
    columns = [i[0] for i in c.description]
    query_df= pd.DataFrame(rows,columns=columns)
    return query_df
def q11(c):  
    c.execute('''select distinct(Hue) from color_table''')

    rows = c.fetchall()
    columns = [i[0] for i in c.description]
    query_df= pd.DataFrame(rows,columns=columns)
    return query_df
def q12(c):  
    c.execute('select Color,Count(Color) from color_table group by Color order by count(Color) desc limit 5')

    rows = c.fetchall()
    columns = [i[0] for i in c.description]
    query_df= pd.DataFrame(rows,columns=columns)
    return query_df
def q13(c):  
    c.execute('select Hue,AVG(Percent) from color_table group by Hue')

    rows = c.fetchall()
    columns = [i[0] for i in c.description]
    query_df= pd.DataFrame(rows,columns=columns)
    return query_df
def q14(c,object_ID):  

    q=('''select Object_ID,distinct Color from color_table where Object_ID= %s''')
    c.execute(q,(object_ID,))

    rows = c.fetchall()
    columns = [i[0] for i in c.description]
    query_df= pd.DataFrame(rows,columns=columns)
    return query_df
def q15(c):  
    c.execute('''select count(Color) from color_table''')

    rows = c.fetchall()
    columns = [i[0] for i in c.description]
    query_df= pd.DataFrame(rows,columns=columns)
    return query_df
def q16(c):  
    c.execute('''select Object_ID,Title,Culture,Hue from metadata inner join color_table on metadata.ID=color_table.Object_ID
    where metadata.Culture="Byzantine" ''')


    rows = c.fetchall()
    columns = [i[0] for i in c.description]
    query_df= pd.DataFrame(rows,columns=columns)
    return query_df
def q17(c):  
    c.execute('''select Object_ID,Title,Hue from metadata inner join color_table on metadata.ID=color_table.Object_ID ''')

    rows = c.fetchall()
    columns = [i[0] for i in c.description]
    query_df= pd.DataFrame(rows,columns=columns)
    return query_df
def q18(c):  
    c.execute('''select Object_ID,Title,Rank_ as Media_Rank,
              Period from metadata inner join media_table on metadata.ID=media_table.Object_ID where metadata.Period!=0 ''')

    rows = c.fetchall()
    columns = [i[0] for i in c.description]
    query_df= pd.DataFrame(rows,columns=columns)
    return query_df
def q19(c):  
    c.execute('''select Title,Rank_ from metadata join media_table on metadata.ID=media_table.Object_ID 
    join color_table on metadata.ID=color_table.Object_ID where Rank_<70 and Hue='Grey' ''')

    rows = c.fetchall()
    columns = [i[0] for i in c.description]
    query_df= pd.DataFrame(rows,columns=columns)
    return query_df
def q20(c):  
    c.execute('''select Classification,count(Classification) as Classification_Count, AVG(Media_Count)as Average_Media_Count from 
    metadata join media_table on metadata.ID=media_table.Object_ID group by Classification ''')

    rows = c.fetchall()
    columns = [i[0] for i in c.description]
    query_df= pd.DataFrame(rows,columns=columns)
    return query_df
def q21(c):  
    c.execute('''select ID,Title,Accession_Year,Accession_Method from 
    metadata where (Accession_Year > 1999) AND (Accession_Method in ('Purchase','Partially Gift/Partially Purchase')) ''')

    rows = c.fetchall()
    columns = [i[0] for i in c.description]
    query_df= pd.DataFrame(rows,columns=columns)
    return query_df
def q22(c):  
    c.execute('''select ID,Title,Rank_ from metadata join media_table on metadata.ID=media_table.Object_ID order by Rank_ ''')  

    rows = c.fetchall()
    columns = [i[0] for i in c.description]
    query_df= pd.DataFrame(rows,columns=columns)
    return query_df
def q23(c):  
    c.execute('''select count(Culture) from metadata where Culture=0 ''')

    rows = c.fetchall()
    columns = [i[0] for i in c.description]
    query_df= pd.DataFrame(rows,columns=columns)
    return query_df
def q24(c):  
    c.execute('''select ID,Title from metadata left join media_table on metadata.ID=media_table.Object_ID 
    where Media_Count=0 and Color_Count=0 and Image_Count=0 ''')

    rows = c.fetchall()
    columns = [i[0] for i in c.description]
    query_df= pd.DataFrame(rows,columns=columns)
    return query_df
def q25(c):  
    c.execute('''select Hue,count(Hue) from color_table group by Hue order by count(Hue) limit 1 ''')

    rows = c.fetchall()
    columns = [i[0] for i in c.description]
    query_df= pd.DataFrame(rows,columns=columns)
    return query_df
def q26(c):  
    c.execute('''select ID,Title,Medium from metadata where Medium='0' ''')

    rows = c.fetchall()
    columns = [i[0] for i in c.description]
    query_df= pd.DataFrame(rows,columns=columns)
    return query_df

def close_c(c,connection):
    
    c.close()
    connection.close()




#------------------------------------------WEB PAGE-------------------------------------------
st.header(":blue[🏛️Harvard’s Artifacts Collection🖼️]")

# c = ["Accessories (non-art)", 'Photographs', 'Drawings', 'Prints', 'Paintings', 'Sculpture',
#     'Coins', 'Vessels', 'Textile Arts', 'Archival Material', 'Fragments',
#     'Manuscripts', 'Seals', 'Straus Materials']
c=['Photographs', 'Drawings', 'Prints',
   'Sculpture', 'Coins', 'Vessels', 'Archival Material', 'Fragments']


st.set_page_config(layout="wide")

if 'options' not in st.session_state or "active_view" not in st.session_state:
    st.session_state.options = c

if 'selected_items' not in st.session_state:
    st.session_state.selected_items = []

if 'prev_selection' not in st.session_state:
    st.session_state.prev_selection = None

if 'button_clicked' not in st.session_state:
    st.session_state.button_clicked = False

if 'sql_inserted' not in st.session_state:
    st.session_state.sql_inserted = False

sql_updated=[]

current_selection = st.selectbox("Select the Required Artifact Name", st.session_state.options)
st.write('The items below have already been inserted into the database')
st.text(st.session_state.selected_items)

try:
    c,connection=connection_f()
    c.execute('select distinct(Classification) from metadata')
    r = c.fetchall()
    classification_sql = [i[0] for i in r]
    close_c(c,connection)

except Exception as e:
    classification_sql=[]
finally:
    st.session_state.selected_items=classification_sql
    st.session_state.options= [i for i in st.session_state.options if i not in st.session_state.selected_items]

if current_selection not in classification_sql:

    if current_selection != st.session_state.prev_selection:
        st.session_state.button_clicked = False 
        st.session_state.active_view = None
        st.session_state.sql_inserted = True  
        st.session_state.active_view = None
    
        st.session_state.prev_selection = current_selection

    if st.button("Get artifact record via API"):
        st.session_state.button_clicked = True

    if st.session_state.button_clicked:
        final_out=get_final_out(current_selection)
        st.success(f"{current_selection} details are received")
            
        metadata=metadata_f(final_out)
        media=media_f(final_out)
        color=color_f(final_out)

        if "active_view" not in st.session_state:
            st.session_state.active_view = None

        def show_view():
            st.session_state.active_view = "view"

        def show_sql():
            st.session_state.active_view = "sql"   

        def show_queries():
            st.session_state.active_view = "queries"


        col1, col2, col3 = st.columns(3)

        with col1:
            st.button("View Data", on_click=show_view) 
            
        with col2:
            st.button("Migrate to SQL", on_click=show_sql)
            
        with col3:
            st.button("Queries", on_click=show_queries)
            

        if st.session_state.active_view == "view":
                
                container1, container2, container3 = st.columns(3)

                with container1:                            
                
                    st.markdown(f"Artifact Metadata of {current_selection}")
                    st.write(metadata)  

                with container2:
            
                    st.markdown(f"Artifact Media of {current_selection}")
                    st.write(media)

                with container3:
                
                    st.markdown(f"Artifact Color of {current_selection}")
                    st.write(color)

       #After data is updated in SQL, the classification dropdown is refreshed automatically.
#Classifications already present in SQL cannot be selected again to prevent duplicate entries and related SQL errors.  
# After completing the above steps, choose any classification, click “Get Artifact Record via API,” and then click “Queries” to proceed.    

        if st.session_state.active_view == "sql":
            st.success(f'{classification_sql} are updated in database.')
          
            metadata_df=metadata_dff(metadata)
            media_df=media_dff(media)
            color_df=color_dff(color)

            st.subheader("Artifact Metadata Table")
            st.dataframe(metadata_df)

            st.subheader("Artifact Media Table")
            st.dataframe(media_df)

            st.subheader("Artifact Color Table")
            st.dataframe(color_df)

          
                    
        if st.session_state.active_view == "queries": 
            questions=questions()

            st.session_state.selected_query = st.selectbox("📝 Select a question", questions)

            if st.session_state.selected_query =="--None--":
                pass

            elif st.session_state.selected_query  ==questions[1]:
                c,connection=connection_f()
                q_df=q1(c)
                st.dataframe(q_df)
                close_c(c,connection)

            elif st.session_state.selected_query  ==questions[2]:
                c,connection=connection_f()
                q_df=q2(c)
                st.dataframe(q_df)
                close_c(c,connection)

            elif st.session_state.selected_query ==questions[3]:
                c,connection=connection_f()
                q_df=q3(c)
                st.dataframe(q_df)
                close_c(c,connection)

            elif st.session_state.selected_query ==questions[4]:
                c,connection=connection_f()
                q_df=q4(c)
                st.dataframe(q_df)
                close_c(c,connection)
            elif st.session_state.selected_query  ==questions[5]:
                c,connection=connection_f()
                q_df=q5(c)
                st.dataframe(q_df)
                close_c(c,connection)
            elif st.session_state.selected_query  ==questions[6]:
                c,connection=connection_f()
                q_df=q6(c)
                st.dataframe(q_df)
                close_c(c,connection)
            elif st.session_state.selected_query ==questions[7]:
                c,connection=connection_f()
                q_df=q7(c)
                st.dataframe(q_df)
                close_c(c,connection)
            elif st.session_state.selected_query  ==questions[8]:
                c,connection=connection_f()
                q_df=q8(c)
                st.dataframe(q_df)
                close_c(c,connection)
            elif st.session_state.selected_query  ==questions[9]:
                c,connection=connection_f()
                q_df=q9(c)
                st.dataframe(q_df)
                close_c(c,connection)
            elif st.session_state.selected_query  ==questions[10]:
                c,connection=connection_f()
                q_df=q10(c)
                st.dataframe(q_df)
                close_c(c,connection)
            elif st.session_state.selected_query  ==questions[11]:
                c,connection=connection_f()
                q_df=q11(c)
                st.dataframe(q_df)
                close_c(c,connection)
            elif st.session_state.selected_query  ==questions[12]:
                c,connection=connection_f()
                q_df=q12(c)
                st.dataframe(q_df)
                close_c(c,connection)
            elif st.session_state.selected_query  ==questions[13]:
                c,connection=connection_f()
                q_df=q13(c)
                st.dataframe(q_df)
                close_c(c,connection)
            elif st.session_state.selected_query  ==questions[14]:

                c,connection=connection_f()
                number = st.number_input("Insert a Object_ID", value=0, placeholder="Type a number...")
                n=int(number)
                st.write("Selected Object ID is ", n)

                q_df=q14(c,number)
                st.dataframe(q_df)
                close_c(c,connection)
            elif st.session_state.selected_query  ==questions[15]:
                c,connection=connection_f()
                q_df=q15(c)
                st.dataframe(q_df)
                close_c(c,connection)

            elif st.session_state.selected_query  ==questions[16]:
                c,connection=connection_f()
                q_df=q16(c)
                st.dataframe(q_df)
                close_c(c,connection)
            elif st.session_state.selected_query  ==questions[17]:
                c,connection=connection_f()
                q_df=q17(c)
                st.dataframe(q_df)
                close_c(c,connection)
            elif st.session_state.selected_query  ==questions[18]:
                c,connection=connection_f()
                q_df=q18(c)
                st.dataframe(q_df)
                close_c(c,connection)
            elif st.session_state.selected_query ==questions[19]:
                c,connection=connection_f()
                q_df=q19(c)
                st.dataframe(q_df)
                close_c(c,connection)
            elif st.session_state.selected_query  ==questions[20]:
                c,connection=connection_f()
                q_df=q20(c)
                st.dataframe(q_df)
                close_c(c,connection)
            elif st.session_state.selected_query  ==questions[21]:
                c,connection=connection_f()
                q_df=q21(c)
                st.dataframe(q_df)
                close_c(c,connection)
            elif st.session_state.selected_query  ==questions[22]:
                c,connection=connection_f()
                q_df=q22(c)
                st.dataframe(q_df)
                close_c(c,connection)
            elif st.session_state.selected_query  ==questions[23]:
                c,connection=connection_f()
                q_df=q23(c)
                st.dataframe(q_df)
                close_c(c,connection)
            elif st.session_state.selected_query  ==questions[24]:
                c,connection=connection_f()
                q_df=q24(c)
                st.dataframe(q_df)
                close_c(c,connection)
            elif st.session_state.selected_query  ==questions[25]:
                c,connection=connection_f()
                q_df=q25(c)
                st.dataframe(q_df)
                close_c(c,connection)
            elif st.session_state.selected_query  ==questions[26]:
                c,connection=connection_f()
                q_df=q26(c)
                st.dataframe(q_df)
                close_c(c,connection)



















    


