import pandas as pd
import kuzu

class Storage:

    def __init__(self):
        # Initialize database and create schema.
        db = kuzu.Database("./demo_db")
        self.k_cient = kuzu.Connection(db)
        

    def clean_ingested_data(self, path):
        # reads the data provided in pandas dataframe and cleans missing values.
        df = pd.read_csv(path)
        df = df.replace('N/A', 0, inplace=True)         # Replace missing values with '0'
        return df
    
    def load_ids(self, path1, path2):
        # path 1 is path to affiliation csv sheet
        # path 2 is path to participants csv sheet
        df1 = self.clean_ingested_data(path1)        # affiliations
        df2 = self.clean_ingested_data(path2)        # participants

        df1 = df1[['ID', 'Title']]
        df2 = df2['Participant-ID']
        
        df1.to_csv("../Datasets/participants", index=False)     # Save cleaned csv file to Datasets folder
        df2.to_csv("../Datasets/affiliations", index=False)

    def load_data(self, path_friend, path_influential, path_feedback, path_moretime, path_advice, path_disrespect, path_schoolactivity):
        friends = self.clean_ingested_data(path_friend)
        influential = self.clean_ingested_data(path_influential)
        feedback = self.clean_ingested_data(path_feedback)
        moretime = self.clean_ingested_data(path_moretime)
        advice = self.clean_ingested_data(path_advice)
        disrespect = self.clean_ingested_data(path_disrespect)
        schoolactivity = self.clean_ingested_data(path_schoolactivity)

        friends.to_csv("../Datasets/friends", index=False)      # Save cleaned csv file to Datasets folder
        influential.to_csv("../Datasets/influential", index=False)
        feedback.to_csv("../Datasets/feedback", index=False)
        moretime.to_csv("../Datasets/moretime", index=False)
        advice.to_csv("../Datasets/advice", index=False)
        disrespect.to_csv("../Datasets/disrespect", index=False)
        schoolactivity.to_csv("../Datasets/schoolactivity", index=False)

    def create_database(self, path1, path2):

        self.load_ids(path1, path2)          # Save cleaned csv files of participants and affiliations

        self.k_client.execute("CREATE NODE TABLE participants(part_ID INT64, title STRING, PRIMARY KEY (part_ID))")
        self.k_client.execute("CREATE NODE TABLE affiliations(aff_ID INT64, PRIMARY KEY (aff_ID))")

        # Friends
        self.k_client.execute("CREATE REL TABLE friends(FROM participants TO participants)")
        
        # influential
        self.k_client.execute("CREATE REL TABLE influential(FROM participants TO participants)")

        # Feedback
        self.k_client.execute("CREATE REL TABLE feedback(FROM participants TO participants)")

        # MoreTime
        self.k_client.execute("CREATE REL TABLE moretime(FROM participants TO participants)")

        # Advice
        self.k_client.execute("CREATE REL TABLE advice(FROM participants TO participants)")

        # Disrespect
        self.k_client.execute("CREATE REL TABLE disrespect(FROM participants TO participants)")

        # SchoolActivity
        self.k_client.execute("CREATE REL TABLE schoolactivity(FROM participants TO affiliations)")

    
    def upload_data(self):
        # Upload data to DB
        # Save data to KuzuDB

        self.k_client.execute('COPY friends FROM "../Datasets/friends.csv"')
        self.k_client.execute('COPY influential FROM "../Datasets/influential.csv"')
        self.k_client.execute('COPY feedback FROM "../Datasets/feedback.csv"')
        self.k_client.execute('COPY moretime FROM "../Datasets/moretime.csv"')
        self.k_client.execute('COPY advice FROM "../Datasets/advice.csv"')
        self.k_client.execute('COPY disrespect FROM "../Datasets/disrespect.csv"')
        self.k_client.execute('COPY schoolactivity FROM "../Datasets/activity.csv"')