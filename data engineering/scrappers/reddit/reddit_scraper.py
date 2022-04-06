# `pip install praw` - Python Reddit API wrapper
# To extract data from Reddit, we need to create a Reddit 
# app. Got to `https://www.reddit.com/prefs/apps`
# Enter name and description while creating, and 
# in the redirect uri box, enter http://localhost:8080


# 2 TYPES OF INSTANCE (The Read-only instance should be enough for our usecase)
# The Authorized instance help to do other stuff like "upvote", "create post", etc

import logging
from secrets import choice
import praw
from praw.models import MoreComments
from sqlalchemy import create_engine, MetaData, Table, insert
from redditconfig import postgresql as config, credentials as cred

reddit_read_only = praw.Reddit(client_id=cred["client_id"],	 # your client id
							   client_secret=cred["client_secret"], # your client secret
							   user_agent=cred["user_agent"])	 # your user agent


# SPECIFYING PARTICULAR SUBREDDIT TO SCRAPE
# asa_name = ["choicecoin"]
# subreddit = reddit_read_only.subreddit(asa_name)

# """
# OTHER IMPORTANT SUBREDDITS ON CRYPTOCURRENCIES

# /r/CryptoCurrencyMemes 
# r/CryptoTechnology 
# r/CryptoMarkets 
# r/CryptoRecruiting
# r/Best_of_Crypto
# r/CryptoTrade
# r/Jobs4Crypto
# r/Liberland 
# r/OpenBazaar 
# r/GPUmining
# * /r/CryptoMarkets
# * /r/CryptoTechnology
# * /r/CryptoCurrencyTrading 
# * /r/defi 
# * /r/CryptoOffers 
# * /r/CryptoSauceNews
# * /r/cryptocurrencies
# """


# ### FILLER CODES
# # Display the name of the Subreddit
# print("Display Name:", subreddit.display_name)

# # Display the title of the Subreddit
# print("Title:", subreddit.title)

# # Display the description of the Subreddit
# print("Description:", subreddit.description)


def scrape_reddit(asa_name:str, comments_table:object, posts_table:object, db_connection:object): 

	""" This function scrapes both the top posts and their respective top comments in a given subreddit.
		asa_name[str]: subreddit name
		comments_table[object]: sql_alchemy object that contains the meta data for the comments table to be updated
		posts_table[Object]: sql_alchemy object that contains the meta data for the posts table to be updated
		db_connection[object]: sql alchemy object connection
	"""

	## connect to subreddit
	subreddit = reddit_read_only.subreddit(asa_name)

	## Get the hot posts in a subreddit
	submissions = subreddit.hot(limit=None)

	post_dict = {} # posts dict
	comment_dict = {} # comments dict

	for post in submissions:

		# getting the comments from each posts.
		for top_level_comment in post.comments:
			# check if it is a comment or an option to check for more comments
			if isinstance(top_level_comment, MoreComments):
				continue
		
			# Text in the comment object
			comment_dict["Body"] = top_level_comment.body
			
			# Unique ID of the comment object
			comment_dict["ID"] = top_level_comment.id

			# Parent ID of the original post
			comment_dict["Parent_ID"] = top_level_comment.parent_id
			
			# The score for the comment
			comment_dict["Score"] = top_level_comment.score
			

			# The time the comment was created
			comment_dict['Time_Created'] = top_level_comment.created_utc

			## Write into the database table for comments
			row = insert(comments_table).values( 
                id=comment_dict['ID'],  
                body=comment_dict["Body"], 
                score=comment_dict["Score"],
				parent_id=comment_dict['Parent_ID'],  
                created_at=comment_dict["Time_Created"]
                )

			# compile query
			row.compile()
			# execute query
			db_connection.execute(row)

		# Title of each post
		post_dict["Title"] = post.title
		
		# Text inside a post
		post_dict["Post_Text"] = post.selftext
		
		# Unique ID of each post
		post_dict["ID"] = post.id
		
		# The score of a post
		post_dict["Score"] = post.score
		
		# Total number of comments inside the post
		post_dict["Total_Comments"] = post.num_comments
		
		# URL of each post
		post_dict["Post_URL"] = post.url

		# The time the post was created
		post_dict['Time_Created'] = post.created_utc

		## Write into the database table for original posts
		row = insert(posts_table).values(
                title=post_dict['Title'], 
                id=post_dict['ID'],  
                post_text=post_dict["Post_Text"], 
                score=post_dict["Score"],
                total_comments=post_dict["Total_Comments"], 
                post_url=post_dict["Post_URL"], 
                time_created=post_dict["Time_Created"]
                )
		# compile query
		row.compile()
		# execute query
		db_connection.execute(row)

# Saving the data in a pandas dataframe
# top_posts = pd.DataFrame(posts_dict)
# print(top_posts.head(10))
# top_posts.to_csv("top_posts.csv", index=True)


### 2ND APPROACH - USING THAT YEYE STREAMING
# for submission in subreddit.stream.submissions():
#     if not submission.stickied:
#         print(str(submission.title) + " " + str(submission.url) + "\n")

# for comment in subreddit.stream.comments():
#     print(str(comment.title) + " " + str(comment.url) + "\n")


### 3RD APPROACH - TRYING TO USE WHILE LOOP TO STREAM 
# seen_submissions = set()

# while True:
#     for submission in subreddit.hot(limit=None):
#         if submission.fullname not in seen_submissions:
#             seen_submissions.add((submission.fullname, "...")) # ADD OTHER RELEVANT INFO TOO
#             print('{} {}\n'.format(submission.title, submission.url))
#     time.sleep(60 * 3)  # sleep for a minute (60 seconds)


### To take care of the comments, 
# https://stackoverflow.com/questions/32413633/how-can-i-make-a-list-of-the-top-comments-in-a-subreddit-with-praw

if __name__ == '__main__':

	## Get logs
	logging.getLogger().setLevel(logging.INFO)

	## Database connecion URL
	url = f"postgresql+pg8000://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['db_name']}"
	
	## Database engine connection
	db_engine = create_engine(url, echo=False)
	db_connection = db_engine.connect()

	logging.info("Database connection created")

	## Table schemas
	table_meta = MetaData(db_engine)
	# posts table
	posts_table = Table(config['Posts_Table'], table_meta, autoload=True)
	# comments table
	comments_table = Table(config['Comments_Table'], table_meta, autoload=True)

	## lists for subreddits' names
	ASAs = ['choicecoin']
	
	for asa in ASAs:
		logging.info("LET THE SCRAPING BEGIN")
		# Scrape based on ASA name
		scrape_reddit(asa, comments_table, posts_table, db_connection)
		logging.info("Database Updated")