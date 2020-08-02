######################################################################################################################################
#           Read Me pls                                                                                                              #
#                                                                                                                                    #
#This program is written to process movies & rating files.                                                                           #
#Below is the execution sequence slong with assumptions:                                                                             #
#-   Prerequisites                                                                                                                   #
#    -   Python installed & running                                                                                                  #
#    -   Python Package Installer - PYPI is installed for on machine.                                                                #
#            If not - pls install using "pip install pandasql"                                                                       #
#            If yes - not required                                                                                                   #
#            If not possible - Alternatively, below code can be used using SQLlite                                                   #
#                                                                                                                                    #
#                import sqlite3                                                                                                      #
#                conn = sqlite3.connect('mydb_file.db')                                                                              #
#                c=conn.cursor()                                                                                                     #
#                                                                                                                                    #
#            And create a relational database structure and use it for processing                                                    #
#                                                                                                                                    #
#            We may also use lambda, map, and apply functions as well                                                                #
#    -   Download the source files and place them appropriately                                                                      #
#                                                                                                                                    #
#    Process Flow:                                                                                                                   #
#        -   Create movies dataframe                                                                                                 #
#        -   Break movie name info into 2 columns - movie name and year                                                              #
#        -   Create a new dataframe to denormalise the genre infomation. See below                                                   #
#            Input                                                                                                                   #
#               Example - movie_id   movie_name  movie_year  genre                                                                   #
#                            1       Avatar      2009           Action|Fantasy|Epic|Sci-Fi                                           #
#            Output                                                                                                                  #
#               Example - movie_id   movie_name  movie_year  genre                                                                   #
#                            1       Avatar      2009           Action                                                               #
#                            1       Avatar      2009           Fantasy                                                              #
#                            1       Avatar      2009           Epic                                                                 #
#                            1       Avatar      2009           Sci-Fi                                                               #
#                                                                                                                                    #
#        -   Roll up users ratings based on below assumptions                                                                        #
#        -   Merge ratings with denormalised movies dataframe using movie_id                                                         #
#        -   Data sorting and order as per below assumptions for sort order and conflict resolution                                  #
#        -   Cross check sorting order and restrict first record for each year                                                       #
#        -   Display output                                                                                                          #
#                                                                                                                                    #
#                                                                                                                                    #
#    Assumptions                                                                                                                     #
#        -   Based on understand Users.dat - file with user id and twitter id is not required hence not processed or merged          #
#        -   Movie ratings from multiple users has been averaged and rolled up at movie id level                                     #
#        -   Sort Order is                                                                                                           #
#                1>>   Year - Desc                                                                                                   #
#                2>>   Genre - Desc                                                                                                  #
#                - Conflict resolution - Mutiple genres having same rating is further sorted by using                                #
#                        1 >> no of users given rating to a movie                                                                    #
#                        2 >> no of movies having same genre                                                                         #
#        -   Output has been displayed as per below 2 columns and filtred for last 10 records.                                       #
#                                                                                                                                    #
######################################################################################################################################            

import pandas as pd 
import pandasql as ps

fileheader = ["movie_id", "movie_name_year", "genre"]
movieslist_df=pd.read_csv('movie.dat',sep='::',engine='python',names=fileheader)
temp1 = movieslist_df["movie_name_year"].str.split("(", n = 1, expand = True) 
temp2= temp1[1].str.split(")",n = 1 , expand = True) 
movieslist_df["movie_name"] = temp1 [0]
movieslist_df["movie_year"] = temp2 [0]
movieslist_df.drop(columns =["movie_name_year"], inplace = True)
movieslist_df.dropna( axis=0, how='any', subset=['genre'], inplace=True)


temp3=movieslist_df["genre"].str.split("|", n = -1, expand = True)
unique_list = []    
for i in range(len(temp3.columns) - 1 ):
    x= list (set(temp3[i].dropna()))
    for val in x:
           if val not in unique_list: 
                unique_list.append(val)
genrelist_df = pd.DataFrame(unique_list)
genrelist_df.columns=['genre']
movies_df_new =pd.DataFrame()
movielist = []
for index, row in movieslist_df.iterrows():
    genre_str =(row["genre"])
    for genre in (genre_str.split('|')):
        a_dict = {'movie_id':row["movie_id"] ,'genre':genre, 'movie_name':row["movie_name"],'movie_year':row["movie_year"]}
        movielist.append(a_dict)
movies_flat_df = movies_df_new.append(movielist,ignore_index=True)   


#fileheader= ["user_id", "twitter_id"]
#userlist_df=pd.read_csv('users.dat',sep='::',engine='python',names=fileheader)

fileheader = ["user_id" , "movie_id", "rating" ,"timestamp"]
ratinglist_df =pd.read_csv('ratings.dat',sep='::',engine='python',names=fileheader)

agguser_sql = """select user_id, movie_id , avg(rating) ttl_rating, count(user_id) no_of_users
        FROM ratinglist_df
        group by user_id, movie_id
         """
users_sql_df = ps.sqldf(agguser_sql, locals())
movies_rating_df = pd.merge(movies_flat_df, users_sql_df, on='movie_id')
#consolidated_df = pd.merge(df_merge_movies_rating,user_df,on='user_id')

aggdata_sql = """select movie_year, genre , avg(ttl_rating) rating, count(no_of_users) no_of_users, count(movie_name) no_of_movies
        FROM movies_rating_df
        group by movie_year, genre
        order by movie_year desc, rating desc, no_of_users desc, no_of_movies desc
         """
orderedmovies_df = ps.sqldf(aggdata_sql, locals())
reorderlist = ['movie_year','rating', 'no_of_users', 'no_of_movies']

output_df = orderedmovies_df.sort_values(reorderlist, ascending=False).drop_duplicates('movie_year').sort_index()

output_df[['movie_year','genre']].head(10).style.hide_index()
