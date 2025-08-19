import streamlit as st
import pandas as pd
import mysql.connector as mysqlcon
import requests
import json
from streamlit_option_menu import option_menu

#MYSQL_Config
conn = mysqlcon.connect(
    host='localhost',
    user='root',
    password='',
    database='guvi_projects'
)
cursor = conn.cursor()


api_key = "47d40efb-2be3-4225-a2a1-e70c53f30c00"

url = "https://api.harvardartmuseums.org/object"

def create_tables():
    cursor.execute(""""create table if not exists artifacts_metadata (
                   id INTEGER primarykey,
                   title TEXT,
                   culture TEXT,
                   period TEXT,
                   century TEXT,
                   medium TEXT,
                   dimensions TEXT,
                   decription TEXT,
                   department TEXT,
                   classification TEXT,
                   accessionyear INTEGER,
                   accessionmethod TEXT
)""")
    cursor.execute("""create table if not exists artifacts_media (
                   objectid INTEGER,
                   imagecount INTEGER,
                   mediacount INTEGER,
                   colorcount INTEGER,
                   rank INTEGER,
                   datebegin INTEGER,
                   dateend INTEGER
                   FOREIGN KEY(objectid)REFERENCES artifacts_metadata(id)
)""")
    cursor.execute("""create table if not exists artifacts_colors (
                   obejctid INTEGER,
                   color TEXT,
                   spectrum TEXT,
                   hue TEXT,
                   percent REAL,
                   css3 TEXT
                   FOREIGN KEY(objectid)REFERENCES artifacts_metadata(id)
)""")
    create_tables()

#___________________________________________________________________DATA COLLETING USING CLASS FUNCTION____________________________________________#

def classes(api_key,class_name):
    all_records = []
   
    for page in range(1, 26):
        params = {
            "apikey": api_key,
            "size": 100,
            "page": page,
            "hasimage": 1,
            "classifcation": class_name
        }
    
        response = requests.get(url, params=params)
        data = response.json()
        records = data.get('records', [])
        all_records.extend(records)
        return all_records
    
#___________________________________________________________________COLLECTING ARTIFACTS DATAS____________________________________________________________#
    
def artifacts_details(records):
    metadata = []
    media = []
    colors = []

    for i in records:
        metadata.append(dict(
            id=i.get('id'),
            title=i.get('title'),
            culture=i.get('culture'),
            period=i.get('period'),
            century=i.get('century'),
            medium=i.get('medium'),
            dimensions=i.get('dimensions'),
            description=i.get('description'),
            department=i.get('department'),
            classification=i.get('classification'),
            accessionyear=i.get('accessionyear'),
            accessionmethod=i.get('accessionmethod')
        ))

        media.append(dict(
            objectid=i.get('objectid'),
            imagecount=i.get('imagecount'),
            mediacount=i.get('mediacount'),
            colorcount=i.get('colorcount'),
            rank=i.get('rank'),
            datebegin=i.get('datebegin'),
            dateend=i.get('dateend')
        ))

        # Proper indentation starts here
        color_details = i.get('colors')
        if color_details:
            for j in color_details:
                colors.append(dict(
                    objectid=j.get('objectid'),
                    color=j.get('color'),
                    spectrum=j.get('spectrum'),
                    hue=j.get('hue'),
                    percent=j.get('percent'),  
                    css3=j.get('css3')))

    return metadata, media, colors


#___________________________________________________________________INSERT DATAS INTO MYSQL________________________________________________________________#

def insert_values(metadata, media, colors):
    insert_metadata = """INSERT INTO artifacts_metadata 
        (id, title, culture, period, century, medium, dimensions, description, department, classification, accessionyear, accessionmethod)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    
    insert_media = """INSERT INTO artifacts_media 
        (objectid, imagecount, mediacount, colorcount, rank, datebegin, dateend)
        VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    
    insert_colors = """INSERT INTO artifacts_colors 
        (objectid, color, spectrum, hue, percent, css3)
        VALUES (%s, %s, %s, %s, %s, %s)"""

    for i in metadata:
        values1 = (
            i['id'], i['title'], i['culture'], i['period'], i['century'],
            i['medium'], i['dimensions'], i['description'],
            i['department'], i['classification'], i['accessionyear'],
            i['accessionmethod']
        )
        cursor.execute(insert_metadata, values1)

    for i in media:
        values2 = (
            i['objectid'], i['imagecount'], i['mediacount'],
            i['colorcount'], i['rank'], i['datebegin'], i['dateend']
        )
        cursor.execute(insert_media, values2)

    for i in colors:
        values3 = (
            i['objectid'], i['color'], i['spectrum'],
            i['hue'], i['percent'], i['css3']
        )
        cursor.execute(insert_colors, values3)

    conn.commit()
print("Data inserted successfully.")

#____________________________________________________________________Stream Lite User Interface________________________________________________#


st.set_page_config(layout="wide")

st.markdown("<h1 style='text-align: center; color: black;'>Harvard Art Museums</h1>", unsafe_allow_html=True)

classification = st.text_input("Enter a classification")
button = st.button("Collect data")

menu = option_menu(
    None,
    ["select your choice", "Migrate to SQL", "SQL Queries"],
    orientation="horizontal"
)

# Collect data
if button:
    if classification != '':
        records = classes(api_key, classification)
        metadata, media, colors = artifacts_details(records)
        c1, c2, c3 = st.columns(3)
        with c1:
            st.header("Metadata")
            st.json(metadata)
        with c2:
            st.header("Media")
            st.json(media)
        with c3:
            st.header("Colors")
            st.json(colors)
    else:
        st.error("Kindly enter a classification")

# Migrate to SQL
if menu == 'Migrate to SQL':
    cursor.execute("SELECT DISTINCT(classification) FROM artifacts_metadata")
    result = cursor.fetchall()
    existing_classes = [i[0] for i in result]

    st.subheader("Insert the collected data")
    if st.button("Insert"):
        if classification not in existing_classes:
            records = classes(api_key, classification)
            metadata, media, colors = artifacts_details(records)
            insert_values(metadata, media, colors)
            st.success("Data inserted successfully")

            st.header("Inserted Data:")
            st.subheader("Artifacts Metadata")
            cursor.execute("SELECT * FROM artifacts_metadata")
            df1 = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
            st.dataframe(df1)

            st.subheader("Artifacts Media")
            cursor.execute("SELECT * FROM artifacts_media")
            df2 = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
            st.dataframe(df2)

            st.subheader("Artifacts Colors")
            cursor.execute("SELECT * FROM artifacts_colors")
            df3 = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
            st.dataframe(df3)
        else:
            st.error("Classification already exists! Kindly try a different classification.")

# SQL Queries
elif menu == "SQL Queries":

    option = st.selectbox(
        "Queries",
        (
            "1.List all artifacts from 11th century belonging to Byzantine culture.",
            "2.What are the unique cultures represented in the artifacts?",
            "3.List all artifacts from the Archaic Period.",
            "4.List artifact titles ordered by accession year in desending order.",
            "5.How many artifacts are there per department?",
            "6. Which artifacts have more than 3 images?",
            "7. What is the average rank of all artifacts?",
            "8. Which artifacts have a higher mediacount than colorcount?",
            "9. List all artifacts created between 1500 and 1600.",
            "10. How many artifacts have no media files?",
            "11. What are all the distinct hues used in the dataset?",
            "12. What are the top 5 most used colors by frequency?",
            "13. What is the average coverage percentage for each hue?",
            "14. List all colors used for a given artifact ID.",
            "15. What is the total number of color entries in the dataset?",
            "16. List artifact titles and hues for all artifacts belonging to the Byzantine culture.",
            "17. List each artifact title with its associated hues.",
            "18. Get artifact titles, cultures, and media ranks where the period is not null.",
            "19. Find artifact titles ranked in the top 10 that include the color hue 'Grey'.",
            "20. How many artifacts exist per classification, and what is the average media count for each?"
            "21. Find the artifact with the highest media count.",
            "22. List all artifacts that have more than one hue assigned.",
            "23. Find the earliest and latest accession years in the dataset.",
            "24. List all cultures with their average artifact rank.",
            "25. Show the top 5 departments with the most artifacts."
        ),
        index=None,
        placeholder="Select a query"
    )

    if option == "1.List all artifacts from the 11th century belonging to Byzantine culture.":
        cursor.execute("""select * from artifacts_metadata where dated = '11th century' and culture = 'Byzantine' """)
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        st.dataframe(df)

    elif option == "2.What are the unique cultures represented in the artifacts?":
        cursor.execute("""select distinct(culture) from artifacts_metadata""")
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        st.dataframe(df)

    elif option == "3.List all artifacts from the Archaic Period.":
        cursor.execute("""select * from artifacts_metadata where period = 'Archaic' """)
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        st.dataframe(df)

    elif option == "4.List artifact titles ordered by accession year in desending order.":
        cursor.execute("""select title from artifacts_metadata order by accessionyear desc""")
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        st.dataframe(df)

    elif option == "5.How many artifacts are there per department?":
        cursor.execute("""select department,count(*) from artifacts_metadata group by department""")
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        st.dataframe(df)

    elif option == "6. Which artifacts have more than 3 images?":
        cursor.execute("""select * from artifacts_media where imagecount > 3""")
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        st.dataframe(df)

    elif option == "7. What is the average rank of all artifacts?":
        cursor.execute("""select avg(rank) from artifacts_media""")
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        st.dataframe(df)

    elif option == "8. Which artifacts have a higher mediacount than colorcount?":
        cursor.execute("""select * from artifacts_media where mediacount > colorcount""")
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        st.dataframe(df)

    elif option == "9. List all artifacts created between 1500 and 1600.":
        cursor.execute("""select * from artifacts_metadata where accessionyear between 1500 and 1600""")
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        st.dataframe(df)

    elif option == "10. How many artifacts have no media files?":
        cursor.execute("""select count(*) from artifacts_media where imagecount = 0""")
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        st.dataframe(df)

    elif option == "11. What are all the distinct hues used in the dataset?":
        cursor.execute("""select distinct(hue) from artifacts_colors""")
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        st.dataframe(df)

    elif option == "12. What are the top 5 most used colors by frequency?":
        cursor.execute("""select color,count(*) from artifacts_colors group by color order by count(*) desc limit 5""")
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        st.dataframe(df)

    elif option == "13. What is the average coverage percentage for each hue?":
        cursor.execute("""select hue,avg(percent) from artifacts_colors group by hue""")
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        st.dataframe(df)

    elif option == "14. List all colors used for a given artifact ID.":
        cursor.execute("""select * from artifacts_colors where objectid = 1""")
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        st.dataframe(df)

    elif option == "15. What is the total number of color entries in the dataset?":
        cursor.execute("""select count(*) from artifacts_colors""")
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        st.dataframe(df)

    elif option == "16. List artifact titles and hues for all artifacts belonging to the Byzantine culture.":
        cursor.execute("""select title,hue from artifacts_metadata 
                          join artifacts_colors on artifacts_metadata.id = artifacts_colors.objectid
                          where culture = 'Byzantine'""")
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        st.dataframe(df)

    elif option == "17. List each artifact title with its associated hues.":
        cursor.execute("""select title,hue from artifacts_metadata 
                          join artifacts_colors on artifacts_metadata.id = artifacts_colors.objectid""")
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        st.dataframe(df)

    elif option == "18. Get artifact titles, cultures, and media ranks where the period is not null.":
        cursor.execute("""select title,culture,rank from artifacts_metadata 
                          join artifacts_media on artifacts_metadata.id = artifacts_media.objectid
                          where period is not null""")
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        st.dataframe(df)

    elif option == "19. Find artifact titles ranked in the top 10 that include the color hue 'Grey'.":
        cursor.execute("""select title from artifacts_metadata 
                          join artifacts_colors on artifacts_metadata.id = artifacts_colors.objectid
                          join artifacts_media on artifacts_metadata.id = artifacts_media.objectid
                          where hue = 'Grey' order by rank asc limit 10""")
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        st.dataframe(df)

    elif option == "20. How many artifacts exist per classification, and what is the average media count for each?":
        cursor.execute("""select classification,count(*),avg(mediacount) 
                          from artifacts_metadata 
                          join artifacts_media on artifacts_metadata.id = artifacts_media.objectid
                          group by classification""")
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        st.dataframe(df)

    elif option == "21. Find the artifact with the highest media count.":
       cursor.execute("""select title, mediacount 
                      from artifacts_metadata 
                      join artifacts_media on artifacts_metadata.id = artifacts_media.objectid
                      order by mediacount desc limit 1""")
       result = cursor.fetchall()
       df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
       st.dataframe(df)

    elif option == "22. List all artifacts that have more than one hue assigned.":
       cursor.execute("""select title, count(distinct hue) as hue_count
                      from artifacts_metadata 
                      join artifacts_colors on artifacts_metadata.id = artifacts_colors.objectid
                      group by title
                      having hue_count > 1""")
       result = cursor.fetchall()
       df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
       st.dataframe(df) 

    elif option == "23. Find the earliest and latest accession years in the dataset.":
       cursor.execute("""select min(accessionyear) as earliest_year, 
                             max(accessionyear) as latest_year
                      from artifacts_metadata""")
       result = cursor.fetchall()
       df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
       st.dataframe(df)  
    
    elif option == "24. List all cultures with their average artifact rank.":
       cursor.execute("""select culture, avg(rank) as avg_rank
                      from artifacts_metadata 
                      join artifacts_media on artifacts_metadata.id = artifacts_media.objectid
                      group by culture""")
       result = cursor.fetchall()
       df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
       st.dataframe(df)

    elif option == "25. Show the top 5 departments with the most artifacts.":
       cursor.execute("""select department, count(*) as artifact_count
                      from artifacts_metadata
                      group by department
                      order by artifact_count desc
                      limit 5""")
       result = cursor.fetchall()
       df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
       st.dataframe(df)


    else:
        st.error("Select a query")
