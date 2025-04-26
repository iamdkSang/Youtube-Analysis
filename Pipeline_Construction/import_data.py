import pandas as pd
import mysql.connector
from mysql.connector import Error

def create_table_videos_day(connection):
    try:
        cursor = connection.cursor()

        create_table_query = """
            CREATE TABLE IF NOT EXISTS videos_day (
                ChannelID VARCHAR(255),
                ChannelName VARCHAR(255),
                VideoID VARCHAR(255),
                Title VARCHAR(4294967295) , 
                Topic VARCHAR(255),
                Duration VARCHAR(255),
                PublishedAt VARCHAR(255),
                Views VARCHAR(255),
                Likes VARCHAR(255),
                NumComments VARCHAR(255),
                Date VARCHAR(255),
                Description VARCHAR(10000),
                Tag VARCHAR(2000)
            );
        """
        cursor.execute(create_table_query)

        print("Table 'videos_day' created successfully")

    except Error as e:
        print("Error while creating table:", e)
        # Handle the error appropriately, such as logging or rolling back changes

    finally:
        if cursor is not None:
            cursor.close()


def import_csv_to_mysql_videos_day(csv_file, table_name, host, port, database, user, password):
    connection = None
    try:
        # Establish MySQL connection
        connection = mysql.connector.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            auth_plugin='mysql_native_password'
        )

        if connection.is_connected():
            # Create table if not exists
            create_table_videos_day(connection)

            cursor = connection.cursor()

            # Read CSV file into a pandas DataFrame
            df = pd.read_csv(csv_file,low_memory=False)
            df['Topic'] = df['Topic'].str.replace("[\[\]']", "", regex=True)
            df['Tag'] = df['Tag'].str.replace("[\[\]']", "", regex=True)

            # Replace spaces in column names with underscores
            df.columns = [col.replace(' ', '_') for col in df.columns]

            # Replace reserved keyword 'Like' with a backtick-enclosed version
            df.columns = [col if col.lower() != 'like' else '`Like`' for col in df.columns]

            # Convert column names to lowercase
            df.columns = [col.lower() for col in df.columns]

            # Modify DataFrame column names to match MySQL table column names
            df.columns = [
                'channelid', 'channelname', 'videoid', 'title', 'topic',
                'duration', 'publishedat', 'views', 'likes',
                'numcomments', 'date','description','tag'
            ]

            # Insert data into MySQL table
            for i, row in df.iterrows():
                row = row.where(pd.notna(row), None)
                columns = ', '.join(df.columns)
                values = ', '.join(['%s'] * len(df.columns))
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
                cursor.execute(sql, tuple(row))

            # Commit changes
            connection.commit()
            print(f"Data from '{csv_file}' imported successfully into MySQL")

    except Error as e:
        print(f"Error while connecting to MySQL or importing data from '{csv_file}'", e)

    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()


def create_table_channels_day(connection):
    try:
        cursor = connection.cursor()

        create_table_query = """
            CREATE TABLE IF NOT EXISTS channels_day (
                ChannelID VARCHAR(255), 
                ChannelName VARCHAR(255),
                Description VARCHAR(10000),
                View BIGINT, 	
                NumSubscriber INT,	
                Topic VARCHAR(255),
                NumVideo INT
            );
        """
        cursor.execute(create_table_query)

        print("Table 'channels_day' created successfully")

    except Error as e:
        print("Error while creating table:", e)

    finally:
        cursor.close()



def import_csv_to_mysql_channels_day(csv_file, table_name, host, port, database, user, password):
    connection = None
    try:
        # Establish MySQL connection
        connection = mysql.connector.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            auth_plugin='mysql_native_password'
        )

        if connection.is_connected():
            # Create table if not exists
            create_table_channels_day(connection)  # Use create_table1 to create 'channels_days' table

            cursor = connection.cursor()

            # Read CSV file into a pandas DataFrame
            df = pd.read_csv(csv_file, encoding='latin1')
            df = df.fillna('default_value')
            df['Topic'] = df['Topic'].str.replace("[\[\]']", "", regex=True)

            # Replace spaces in column names with underscores
            df.columns = [col.replace(' ', '_') for col in df.columns]

            # Replace reserved keyword 'Like' with a backtick-enclosed version
            df.columns = [col if col.lower() != 'like' else '`Like`' for col in df.columns]

            # Convert column names to lowercase
            df.columns = [col.lower() for col in df.columns]

            # Modify DataFrame column names to match MySQL table column names
            df.columns = [
                'ChannelID', 'ChannelName', 'Description', 'View', 'NumSubscriber', 'Topic', 'NumVideo'
            ]

            # Insert data into MySQL table
            for i, row in df.iterrows():
                columns = ', '.join(df.columns)
                values = ', '.join(['%s'] * len(df.columns))
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
                cursor.execute(sql, tuple(row))

            # Commit changes
            connection.commit()
            print(f"Data from '{csv_file}' imported successfully into MySQL")

    except Error as e:
        print(f"Error while connecting to MySQL or importing data from '{csv_file}'", e)

    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()


if __name__ == "__main__":
    # List of CSV files to import
    csv_files = ["dataNew_P1_2.csv","dataNew_P1_3.csv"]  # Add more files as needed
    csv_files_1 = ['channels_day_1.csv','channels_day_2.csv','channels_day_3.csv','channels_day_4.csv','channels_day_5.csv']
    table_name_videos_day = "videos_day"
    table_name_channels_day = "channels_day"
    port = "3307"
    host = "localhost"
    database = "youtube"
    user = "admin"
    password = "admin123"

    for csv_file in csv_files:
        import_csv_to_mysql_videos_day(csv_file, table_name_videos_day, host, port, database, user, password)

    # for csv_file in csv_files_1:
    #     import_csv_to_mysql_channels_day(csv_file, table_name_channels_day, host, port, database, user, password)