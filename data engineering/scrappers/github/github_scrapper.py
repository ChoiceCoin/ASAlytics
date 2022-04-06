## Imports
import logging
from operator import itemgetter
from github import Github
from sqlalchemy import create_engine, exists, MetaData, Table, insert
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy_utils import database_exists, create_database
from github_setup import postgresql as db_config, github as github_config


ACCESS_TOKEN = github_config['access_token']

g = Github(ACCESS_TOKEN)
            

class Github_api():
    def __init__(self, repo):
        self.repo= repo

    def pull_requests(self):
        repo= self.repo
        count_of_pr= 0
        pr_dict= {"pr_name": [], "pr_count":[]}
        pulls = repo.get_pulls(state= "open", sort= "created")
        for i in pulls:
            count_of_pr += 1
            pr_dict["pr_name"].append(i)
        pr_dict["pr_count"].append(count_of_pr)
        return pr_dict

    def issues(self):
        repo= self.repo
        issues_dict= {}
        count_of_issues= 0
        issues= repo.get_issues(state= "open")
        issues_dict["issues_names"]= issues.get_page(0)
        for i in issues:
            count_of_issues +=1 
        issues_dict["issues_counts"]= count_of_issues
        return issues_dict

    def commits(self):
        repo= self.repo
        count_of_commit= 0
        commit = repo.get_commits()
        for i in commit:
            count_of_commit += 1
        return count_of_commit

    def contributors_count(self):
        repo= self.repo
        count_of_contributors= 0
        contributors = repo.get_contributors()
        for i in contributors:
            count_of_contributors += 1
        return count_of_contributors

    def analyze_traffic(self):
        repo= self.repo
        watch_dict= {}
        clones = repo.get_clones_traffic(per="day")
        views = repo.get_views_traffic(per="day")
        best_day = max(*list((day.count, day.timestamp) for day in views["views"]), key=itemgetter(0))
        watch_dict.update({"views": views, "clones_count": clones["count"], "unique_clones": clones["uniques"],
            "views_count": views["count"], "unique_views": views["uniques"], "highest_views": best_day[0],
            "day_of_highest_views": best_day[1]})
        return watch_dict

    def get_data(self):
        repo= self.repo
        try:
            data= {}
            data['Repo_name']= repo.full_name
            data["Repo_desc"]= repo.description
            x = repo.created_at
            data["Date_created"]= x.strftime("%Y %m %d  %H:%M:%S")
            x = repo.pushed_at
            data["Last_push_date"]= x.strftime("%Y %m %d  %H:%M:%S")
            data["Language"]= repo.language
            data["Number_of_forks"]= repo.forks
            data["Number_of_stars"] = repo.stargazers_count
            data['watchers'] = repo.watchers_count
            data['Number_of_contributors']= Github_api(repo).contributors_count()
            data["Number_of_commits"]= Github_api(repo).commits()
            data["Issues"]= Github_api(repo).issues()['issues_counts']
            x = Github_api(repo).pull_requests()['pr_count']
            data["Pull_Requests"] = x.pop()
            return data

            # print(data)

        except AttributeError as error:
            print ("Error:", error)

# for repo in user.get_repos():
#     create_engine= Github_api(repo)
#     print(create_engine.get_data())
#     print("="*100)



def main():

    usernames = ['choiceCoin']

    url = f"postgresql+pg8000://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['db_name']}"

    db_engine = create_engine(url, echo=False)
    table_meta = MetaData(db_engine)
    table = Table(db_config['table'], table_meta, autoload=True)

    for username in usernames:
        
        g = Github(ACCESS_TOKEN)
        user = g.get_user(username)
        db_connection = db_engine.connect()


        for repo in user.get_repos():
            github_engine = Github_api(repo)
            data = github_engine.get_data()
           
            row = insert(table).values(
                repo_name=data['Repo_name'], 
                repo_desc=data['Repo_desc'],  
                date_created=data["Date_created"], 
                last_push_date=data["Last_push_date"],
                language=data["Language"], 
                no_of_forks=data["Number_of_forks"], 
                no_of_stars=data["Number_of_stars"], 
                no_of_watchers=data['watchers'],
                no_of_contributors=data['Number_of_contributors'],
                no_of_commits=data["Number_of_commits"], 
                issues=data['Issues'],  
                pull_requests=data["Pull_Requests"]
                )

            row.compile()
            db_connection.execute(row)
        print("Database updated")
        db_connection.close()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()


