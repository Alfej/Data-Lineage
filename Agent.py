from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI
import pandas as pd
from langchain_community.document_loaders.csv_loader import CSVLoader
from langchain_core.prompts import ChatPromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.runnables import RunnablePassthrough


loader = CSVLoader(file_path="test2.csv")
data = loader.load()
csv_content_string = ""
for doc in data:
    csv_content_string += doc.page_content + "\n"

load_dotenv()

model = ChatGoogleGenerativeAI(
            model="gemini-2.5-pro",
            temperature=1
        )


prompt = ChatPromptTemplate.from_messages([
    ("system", "You are a data analyst analyzing table dependencies in SQL queries.Below is a CSV file containing a list of tables and their relationships and provide insights on the data "),
    ("human", "Here is the CSV data:\n{csv_data}")
])


# model = ChatGoogleGenerativeAI(model="gemini-2.5-pro") # Or your chosen Gemini model
print(prompt.invoke({"csv_data": csv_content_string}))
chain =  prompt | model

response = chain.invoke({"csv_data": csv_content_string})
print(response)