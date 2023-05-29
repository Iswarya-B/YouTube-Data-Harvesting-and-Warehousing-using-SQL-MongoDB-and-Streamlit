# YouTube-Data-Harvesting-and-Warehousing-using-SQL-MongoDB-and-Streamlit
This project involves the extraction of data from YouTube, specifically focusing on video-related information. The extracted data includes details such as channel information, video name, video ID, view count, like count, favorite count, and posting date. Additionally, all comment details are also collected.

To accomplish this, several packages are utilized:

Google apiclient.discovery: This package allows interaction with the YouTube Data API, enabling the retrieval of video-related data.

Streamlit: Streamlit is employed as the web application framework for this project. It provides an intuitive and user-friendly interface for easy interaction with the application.

psycopg2: This package is utilized to establish a connection and perform operations on a PostgreSQL database. It facilitates the migration of data from MongoDB to PostgreSQL for subsequent analysis.

pandas: The pandas package is used for data manipulation and analysis. It aids in organizing and structuring the collected data in a convenient format.

pymongo: This package is employed to interact with MongoDB, a NoSQL database. It enables the storage of unstructured data extracted from YouTube.

The workflow of the project involves first extracting the desired data from YouTube using the Google apiclient.discovery package. The extracted data, which consists of both structured and unstructured information, is then stored in MongoDB.

Next, the non-structured data is migrated from MongoDB to a PostgreSQL database using the psycopg2 package. This allows for efficient analysis and querying of the data in a structured manner.

To provide an easy and user-friendly interface, the entire application is developed using Streamlit. Users can interact with the application, access the stored data, and perform various analyses as per their requirements.

Lastly, the project includes the provision of the top 10 questions and their corresponding answers for analysis purposes. These questions can be used as a starting point to gain insights from the collected data.

Overall, this project combines data extraction, storage in both MongoDB and PostgreSQL, and analysis using Streamlit, providing users with a comprehensive and interactive platform to explore YouTube video-related information.
