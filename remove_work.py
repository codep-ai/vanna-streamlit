import dataclasses
import json
from typing import Callable, List, Tuple, Union

import requests
import pandas as pd
from io import StringIO
import sys
sys.path.append('/home/ec2-user/git/vanna-streamlit')  # Replace '/path/to/directory' with the actual directory path

from connect_db import connect_to_db

from vanna.base import VannaBase
from vanna.types import (
    AccuracyStats,
    ApiKey,
    DataFrameJSON,
    DataResult,
    Explanation,
    FullQuestionDocument,
    NewOrganization,
    NewOrganizationMember,
    Organization,
    OrganizationList,
    PlotlyResult,
    Question,
    QuestionCategory,
    QuestionId,
    QuestionList,
    QuestionSQLPair,
    QuestionStringList,
    SQLAnswer,
    Status,
    StatusWithId,
    StringData,
    TrainingData,
    UserEmail,
    UserOTP,
    Visibility,
)


class VannaDefault(VannaBase):
    def __init__(self, model: str, api_key: str, config=None):
        VannaBase.__init__(self, config=config)

        self._model = model
        self._api_key = api_key
        self.language = "english"

        self._endpoint = (
            "https://ask.vanna.ai/rpc"
            if config is None or "endpoint" not in config
            else config["endpoint"]
        )
        self._unauthenticated_endpoint = (
            "https://ask.vanna.ai/unauthenticated_rpc"
            if config is None or "unauthenticated_endpoint" not in config
            else config["unauthenticated_endpoint"]
        )

    def _unauthenticated_rpc_call(self, method, params):
        headers = {
            "Content-Type": "application/json",
        }
        data = {
            "method": method,
            "params": [self._dataclass_to_dict(obj) for obj in params],
        }

        response = requests.post(
            self._unauthenticated_endpoint, headers=headers, data=json.dumps(data)
        )
        return response.json()

    def _rpc_call(self, method, params):
        if method != "list_orgs":
            headers = {
                "Content-Type": "application/json",
                "Vanna-Key": self._api_key,
                "Vanna-Org": self._model,
            }
        else:
            headers = {
                "Content-Type": "application/json",
                "Vanna-Key": self._api_key,
                "Vanna-Org": "demo-tpc-h",
            }

        data = {
            "method": method,
            "params": [self._dataclass_to_dict(obj) for obj in params],
        }

        response = requests.post(self._endpoint, headers=headers, data=json.dumps(data))
        return response.json()

    def connect_db(selected_db: str):
        conn =connect_to_db(selected_db)
        return conn

    def is_sql_valid(self, sql: str, selected_db: str):
        try:
            # Connect to the selected database
            conn = connect_to_db(selected_db)

            # Execute the query
            cursor = conn.cursor()

            # set default schema for each connection
            if selected_db =='Snowflake':
                database=st.secrets["SNOWFLAKE_DATABASE"]
                schema=st.secrets["SNOWFLAKE_SCHEMA"]
                #warehouse=st.secrets["SNOWFLAKE_WAREHOUSE"]
                role=st.secrets["SNOWFLAKE_ROLE"]
                cursor.execute(f"USE ROLE {role}")
                #cursor.execute(f"USE WAREHOUSE {warehouse}")
                cursor.execute(f"USE DATABASE {database}")
            elif selected_db =='Redshift':
                schema=st.secrets["REDSHIFT_SCHEMA"]

                # Set the schema
                cursor.execute(f'SET search_path TO {schema};')
            else:
                database='datapai'
                schema='datapai'
            cursor.execute(sql)

            # Close the connection
            conn.close()

            return True
        except Exception as e:
            print(f"SQL Validation Error: {e}")
            return False

    def is_sql_valid_sqlite(self, sql: str):
        try:
            # Here, you would actually execute the query in a safe way to check its validity
            # For example, if using SQLite:
            import sqlite3
            connection = sqlite3.connect('/home/ec2-user/git/vanna-streamlit/Chinook.sqlite')  # or your actual database connection
            cursor = connection.cursor()
            cursor.execute(sql)
            connection.close()
            return True
        except Exception as e:
            print(f"SQL Validation Error: {e}")
            return False

    def should_generate_chart(self, df: pd.DataFrame) -> bool:
        """
        Example:
        ```python
        vn.should_generate_chart(df)
        ```

        Checks if a chart should be generated for the given DataFrame. By default, it checks if the DataFrame has more than one row and has numerical columns.
        You can override this method to customize the logic for generating charts.

        Args:
            df (pd.DataFrame): The DataFrame to check.

        Returns:
            bool: True if a chart should be generated, False otherwise.
        """

        if len(df) > 1 and df.select_dtypes(include=['number']).shape[1] > 0:
            return True

        return False
    def generate_summary(self, question: str, df: pd.DataFrame, **kwargs) -> str:
        """
        **Example:**
        ```python
        vn.generate_summary("What are the top 10 customers by sales?", df)
        ```

        Generate a summary of the results of a SQL query.

        Args:
            question (str): The question that was asked.
            df (pd.DataFrame): The results of the SQL query.

        Returns:
            str: The summary of the results of the SQL query.
        """

        message_log = [
            self.system_message(
                f"You are a helpful data assistant. The user asked the question: '{question}'\n\nThe following is a pandas DataFrame with the results of the query: \n{df.to_markdown()}\n\n"
            ),
            self.user_message(
                "Briefly summarize the data based on the question that was asked. Do not respond with any additional explanation beyond the summary." +
                self._response_language()
            ),
        ]

        summary = self.submit_prompt(message_log, **kwargs)

        return summary

    def user_message(self, message: str) -> any:
        return {"role": "user", "content": message}

    def _response_language(self) -> str:
        if self.language is None:
            return ""

        return f"Respond in the {self.language} language."

    def system_message(self, message: str) -> any:
        return {"role": "system", "content": message}

    def _dataclass_to_dict(self, obj):
        return dataclasses.asdict(obj)

    def get_training_data(self, **kwargs) -> pd.DataFrame:
        """
        Get the training data for the current model

        **Example:**
        ```python
        training_data = vn.get_training_data()
        ```

        Returns:
            pd.DataFrame or None: The training data, or None if an error occurred.

        """
        params = []

        d = self._rpc_call(method="get_training_data", params=params)

        if "result" not in d:
            return None

        # Load the result into a dataclass
        training_data = DataFrameJSON(**d["result"])

        df = pd.read_json(StringIO(training_data.data))

        return df

    def remove_training_data(self, id: str, **kwargs) -> bool:
        """
        Remove training data from the model

        **Example:**
        ```python
        vn.remove_training_data(id="1-ddl")
        ```

        Args:
            id (str): The ID of the training data to remove.
        """
        params = [StringData(data=id)]

        d = self._rpc_call(method="remove_training_data", params=params)

        if "result" not in d:
            raise Exception(f"Error removing training data")

        status = Status(**d["result"])

        if not status.success:
            raise Exception(f"Error removing training data: {status.message}")

        return status.success

    def generate_questions(self) -> List[str]:
        """
        **Example:**
        ```python
        vn.generate_questions()
        # ['What is the average salary of employees?', 'What is the total salary of employees?', ...]
        ```

        Generate questions using the Vanna.AI API.

        Returns:
            List[str] or None: The questions, or None if an error occurred.
        """
        d = self._rpc_call(method="generate_questions", params=[])

        if "result" not in d:
            return None

        # Load the result into a dataclass
        question_string_list = QuestionStringList(**d["result"])

        return question_string_list.questions

    def add_ddl(self, ddl: str, **kwargs) -> str:
        """
        Adds a DDL statement to the model's training data

        **Example:**
        ```python
        vn.add_ddl(
            ddl="CREATE TABLE employees (id INT, name VARCHAR(255), salary INT)"
        )
        ```

        Args:
            ddl (str): The DDL statement to store.

        Returns:
            str: The ID of the DDL statement.
        """
        params = [StringData(data=ddl)]

        d = self._rpc_call(method="add_ddl", params=params)

        if "result" not in d:
            raise Exception("Error adding DDL", d)

        status = StatusWithId(**d["result"])

        return status.id

    def add_documentation(self, documentation: str, **kwargs) -> str:
        """
        Adds documentation to the model's training data

        **Example:**
        ```python
        vn.add_documentation(
            documentation="Our organization's definition of sales is the discount price of an item multiplied by the quantity sold."
        )
        ```

        Args:
            documentation (str): The documentation string to store.

        Returns:
            str: The ID of the documentation string.
        """
        params = [StringData(data=documentation)]

        d = self._rpc_call(method="add_documentation", params=params)

        if "result" not in d:
            raise Exception("Error adding documentation", d)

        status = StatusWithId(**d["result"])

        return status.id

    def add_question_sql(self, question: str, sql: str, **kwargs) -> str:
        """
        Adds a question and its corresponding SQL query to the model's training data. The preferred way to call this is to use [`vn.train(sql=...)`][vanna.train].

        **Example:**
        ```python
        vn.add_sql(
            question="What is the average salary of employees?",
            sql="SELECT AVG(salary) FROM employees"
        )
        ```

        Args:
            question (str): The question to store.
            sql (str): The SQL query to store.
            tag (Union[str, None]): A tag to associate with the question and SQL query.

        Returns:
            str: The ID of the question and SQL query.
        """
        if "tag" in kwargs:
            tag = kwargs["tag"]
        else:
            tag = "Manually Trained"

        params = [QuestionSQLPair(question=question, sql=sql, tag=tag)]

        d = self._rpc_call(method="add_sql", params=params)

        if "result" not in d:
            raise Exception("Error adding question and SQL pair", d)

        status = StatusWithId(**d["result"])

        return status.id

    def generate_embedding(self, data: str, **kwargs) -> List[float]:
        """
        Not necessary for remote models as embeddings are generated on the server side.
        """
        pass

    def generate_plotly_code(
        self, question: str = None, sql: str = None, df_metadata: str = None, **kwargs
    ) -> str:
        """
        **Example:**
        ```python
        vn.generate_plotly_code(
            question="What is the average salary of employees?",
            sql="SELECT AVG(salary) FROM employees",
            df_metadata=df.dtypes
        )
        # fig = px.bar(df, x="name", y="salary")
        ```
        Generate Plotly code using the Vanna.AI API.

        Args:
            question (str): The question to generate Plotly code for.
            sql (str): The SQL query to generate Plotly code for.
            df (pd.DataFrame): The dataframe to generate Plotly code for.
            chart_instructions (str): Optional instructions for how to plot the chart.

        Returns:
            str or None: The Plotly code, or None if an error occurred.
        """
        if kwargs is not None and "chart_instructions" in kwargs:
            if question is not None:
                question = (
                    question
                    + " -- When plotting, follow these instructions: "
                    + kwargs["chart_instructions"]
                )
            else:
                question = (
                    "When plotting, follow these instructions: "
                    + kwargs["chart_instructions"]
                )

        params = [
            DataResult(
                question=question,
                sql=sql,
                table_markdown=df_metadata,
                error=None,
                correction_attempts=0,
            )
        ]

        d = self._rpc_call(method="generate_plotly_code", params=params)

        if "result" not in d:
            return None

        # Load the result into a dataclass
        plotly_code = PlotlyResult(**d["result"])

        return plotly_code.plotly_code

    def generate_question(self, sql: str, **kwargs) -> str:
        """

        **Example:**
        ```python
        vn.generate_question(sql="SELECT * FROM students WHERE name = 'John Doe'")
        # 'What is the name of the student?'
        ```

        Generate a question from an SQL query using the Vanna.AI API.

        Args:
            sql (str): The SQL query to generate a question for.

        Returns:
            str or None: The question, or None if an error occurred.

        """
        params = [
            SQLAnswer(
                raw_answer="",
                prefix="",
                postfix="",
                sql=sql,
            )
        ]

        d = self._rpc_call(method="generate_question", params=params)

        if "result" not in d:
            return None

        # Load the result into a dataclass
        question = Question(**d["result"])

        return question.question

    def get_sql_prompt(
        self,
        question: str,
        question_sql_list: list,
        ddl_list: list,
        doc_list: list,
        **kwargs,
    ):
        """
        Not necessary for remote models as prompts are generated on the server side.
        """

    def get_followup_questions_prompt(
        self,
        question: str,
        df: pd.DataFrame,
        question_sql_list: list,
        ddl_list: list,
        doc_list: list,
        **kwargs,
    ):
        """
        Not necessary for remote models as prompts are generated on the server side.
        """

    def submit_prompt(self, prompt, **kwargs) -> str:
        """
        Not necessary for remote models as prompts are handled on the server side.
        """

    def get_similar_question_sql(self, question: str, **kwargs) -> list:
        """
        Not necessary for remote models as similar questions are generated on the server side.
        """

    def get_related_ddl(self, question: str, **kwargs) -> list:
        """
        Not necessary for remote models as related DDL statements are generated on the server side.
        """

    def get_related_documentation(self, question: str, **kwargs) -> list:
        """
        Not necessary for remote models as related documentation is generated on the server side.
        """

    def generate_sql(self, question: str, **kwargs) -> str:
        """
        **Example:**
        ```python
        vn.generate_sql_from_question(question="What is the average salary of employees?")
        # SELECT AVG(salary) FROM employees
        ```

        Generate an SQL query using the Vanna.AI API.

        Args:
            question (str): The question to generate an SQL query for.

        Returns:
            str or None: The SQL query, or None if an error occurred.
        """
        params = [Question(question=question)]

        d = self._rpc_call(method="generate_sql_from_question", params=params)

        if "result" not in d:
            return None

        # Load the result into a dataclass
        sql_answer = SQLAnswer(**d["result"])

        return sql_answer.sql

    def generate_sql_all(self, question: str, selected_db: str, **kwargs) -> str:
        """
        **Example:**
        ```python
        vn.generate_sql_from_question(question="What is the average salary of employees?", selected_db="Snowflake")
        # SELECT AVG(salary) FROM employees
        ```

        Generate an SQL query using the Vanna.AI API.

        Args:
            question (str): The question to generate an SQL query for.
            selected_db (str): The selected database (e.g., "Snowflake", "Redshift", "SQLite3", "DuckDB").

        Returns:
            str or None: The SQL query, or None if an error occurred.
        """
        params = [Question(question=question, selected_db=selected_db)]  # Pass selected_db parameter to the API

        d = self._rpc_call(method="generate_sql_from_question", params=params)

        if "result" not in d:
            return None

        # Load the result into a dataclass
        sql_answer = SQLAnswer(**d["result"])

        return sql_answer.query

    def generate_followup_questions(self, question: str, df: pd.DataFrame, **kwargs) -> List[str]:
        """
        **Example:**
        ```python
        vn.generate_followup_questions(question="What is the average salary of employees?", df=df)
        # ['What is the average salary of employees in the Sales department?', 'What is the average salary of employees in the Engineering department?', ...]
        ```

        Generate follow-up questions using the Vanna.AI API.

        Args:
            question (str): The question to generate follow-up questions for.
            df (pd.DataFrame): The DataFrame to generate follow-up questions for.

        Returns:
            List[str] or None: The follow-up questions, or None if an error occurred.
        """
        params = [
            DataResult(
                question=question,
                sql=None,
                table_markdown="",
                error=None,
                correction_attempts=0,
            )
        ]

        d = self._rpc_call(method="generate_followup_questions", params=params)

        if "result" not in d:
            return None

        # Load the result into a dataclass
        question_string_list = QuestionStringList(**d["result"])

        return question_string_list.questions        
