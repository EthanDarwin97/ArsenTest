import sqlite3
import re
import json
import logging
from datetime import datetime
from typing import List, Tuple, Dict, Any, Optional

# Configure structured logging for production observability
# Compliance: ISO 27001 / SOC2 requirement for audit trails
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ArsenSQLAgent:
    def __init__(self, db_path: str = ":memory:"):
        """
        Initialize the AI SQL Agent with a database connection and schema metadata.
        Target: Banking/Fintech standard for secure data access.
        """
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self._setup_database()
        
        # Configuration for the mock LLM behavior
        self.model_name = "gpt-4-turbo"
        self.schema_info = """
        Table: customers
        - id (INTEGER, PRIMARY KEY)
        - name (TEXT)
        - email (TEXT)
        - country (TEXT)
        - signup_date (DATE)
        
        Table: orders
        - id (INTEGER, PRIMARY KEY)
        - customer_id (INTEGER, FOREIGN KEY references customers.id)
        - product_name (TEXT)
        - amount (FLOAT)
        - order_date (DATE)
        """

    def _setup_database(self):
        """Initializes schema and mock data for testing purposes."""
        self.cursor.executescript("""
            CREATE TABLE customers (
                id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT,
                country TEXT,
                signup_date DATE
            );
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                product_name TEXT,
                amount FLOAT,
                order_date DATE,
                FOREIGN KEY (customer_id) REFERENCES customers (id)
            );
        """)
        # Insert mock records for demonstration
        self.cursor.executescript("""
            INSERT INTO customers VALUES (1, 'John Doe', 'john@example.com', 'USA', '2023-01-01');
            INSERT INTO customers VALUES (2, 'Jane Smith', 'jane@example.jp', 'Japan', '2023-05-10');
            INSERT INTO orders VALUES (101, 1, 'Cloud Subscription', 500.0, '2023-11-20');
            INSERT INTO orders VALUES (102, 1, 'Consulting Fee', 1500.0, '2023-12-01');
            INSERT INTO orders VALUES (103, 2, 'Software License', 2000.0, '2023-12-05');
        """)
        self.conn.commit()

    # --------------------------------------------------------------------------
    # 1. NATURAL LANGUAGE -> SQL (Prompt Strategy)
    # --------------------------------------------------------------------------
    def generate_sql(self, question: str) -> str:
        """
        Converts natural language to SQL using prompt engineering.
        Strategy: Few-shot prompting and Schema Pinning to reduce hallucination.
        """
        prompt = f"""
        ### ROLE: Senior SQL Expert
        ### TASK: Convert the user question into a valid SQLite query based on the schema below.
        
        ### SCHEMA:
        {self.schema_info}
        
        ### CONSTRAINTS:
        1. Output ONLY the raw SQL query. No markdown formatting.
        2. Only use SELECT statements.
        3. Join tables using customer_id = customers.id where necessary.
        4. Use standard SQLite functions for dates and aggregations.
        5. Hallucination Guard: Only use columns defined in the schema above.
        
        ### QUESTION: {question}
        ### SQL:"""

        # Mock LLM Logic for testing purposes
        # In production: Replace with actual LLM API call (e.g., OpenAI/Anthropic)
        q = question.lower()
        if "spending" in q or "top customer" in q:
            return "SELECT c.name, SUM(o.amount) FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.id ORDER BY SUM(o.amount) DESC LIMIT 1"
        elif "all customers" in q:
            return "SELECT * FROM customers"
        elif "drop" in q or "delete" in q: 
            return "DROP TABLE customers" # Simulating a prompt injection attempt
        
        return "SELECT count(*) FROM orders"

    # --------------------------------------------------------------------------
    # 2. SQL VALIDATION LAYER (Guardrails)
    # --------------------------------------------------------------------------
    def validate_sql_safety(self, sql: str) -> Tuple[bool, Optional[str]]:
        """
        Enforces security policies on generated SQL queries.
        Compliance: OWASP Top 10 for LLM (Prevention of Insecure Output Handling).
        """
        sql_upper = sql.upper().strip()
        
        # Policy: Read-only access
        if not sql_upper.startswith("SELECT"):
            return False, "Access Denied: Only SELECT statements are permitted."
        
        # Policy: Forbidden DDL/DML keywords to prevent data loss or injection
        forbidden = ["DROP", "DELETE", "UPDATE", "INSERT", "TRUNCATE", "ALTER", "CREATE", "GRANT"]
        if any(re.search(rf"\b{word}\b", sql_upper) for word in forbidden):
            return False, "Security Violation: Unauthorized keywords detected in query."
            
        return True, None

    # --------------------------------------------------------------------------
    # 3. SQL EXECUTION (Data Handling)
    # --------------------------------------------------------------------------
    def execute_query(self, sql: str) -> Tuple[List[Tuple], Optional[str]]:
        """Executes the validated SQL query and handles database-level errors."""
        try:
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            return results, None
        except sqlite3.Error as e:
            return [], f"Database Error: {str(e)}"

    # --------------------------------------------------------------------------
    # 4. SQL RESULT -> NATURAL LANGUAGE (Interpretation)
    # --------------------------------------------------------------------------
    def interpret_result(self, question: str, sql: str, rows: List[Tuple]) -> str:
        """Translates raw data into a human-readable summary while avoiding data leakage."""
        if not rows:
            return "I couldn't find any records matching your request."
        
        # Logic to summarize the result set
        # In production: This part is usually handled by a secondary LLM call (Summarization)
        count = len(rows)
        sample = rows[0]
        return f"Based on the database, I found {count} relevant result(s). For instance: {sample}."

    # --------------------------------------------------------------------------
    # MAIN ENTRY POINT
    # --------------------------------------------------------------------------
    def ask(self, question: str) -> Dict[str, Any]:
        """Orchestrates the NL-to-Answer pipeline."""
        logger.info(f"Incoming Request: {question}")
        
        # Step 1: Text to SQL
        generated_sql = self.generate_sql(question)
        
        # Step 2: Safety Check
        is_safe, error_msg = self.validate_sql_safety(generated_sql)
        if not is_safe:
            return {
                "question": question,
                "generated_sql": generated_sql,
                "answer": error_msg,
                "row_count": 0,
                "confidence": 0.0
            }
        
        # Step 3: Execute
        rows, exec_error = self.execute_query(generated_sql)
        if exec_error:
            return {
                "question": question,
                "generated_sql": generated_sql,
                "answer": exec_error,
                "row_count": 0,
                "confidence": 0.0
            }
            
        # Step 4: Natural Language Answer
        answer = self.interpret_result(question, generated_sql, rows)
        
        return {
            "question": question,
            "generated_sql": generated_sql,
            "answer": answer,
            "row_count": len(rows),
            "confidence": 0.95 # Mock confidence score based on logprobs
        }

# --- OPTIONAL QUESTIONS & ARCHITECTURAL DESIGN ---
"""
1. How to reduce hallucinated columns?
   - Implement 'Schema Pruning': Only inject the relevant subset of the schema into the prompt using vector-based metadata search.
   - Include 'Column Descriptions' and 'Value Examples' in the prompt to ground the LLM's understanding of data types.
   - Use a 'Self-Correction' loop where the LLM validates its generated SQL against the actual DDL.

2. Temperature = 0?
   - Yes. For structured output tasks like SQL generation, determinism is critical. Temperature 0 ensures the model picks the highest-probability token, reducing "creative" but syntactically incorrect queries.

3. System Evaluation:
   - Use 'Execution Accuracy': Compare the result set of the generated SQL against a "Golden SQL" dataset.
   - Use 'LLM-as-a-Judge': A more capable model (e.g., GPT-4o) evaluates the logical equivalence between the NL question and the generated SQL.

4. Scaling for Production:
   - Implement 'Semantic Caching' (e.g., using Redis/Faiss) to store and reuse results for similar natural language queries.
   - Connect the Agent to 'Read Replicas' to isolate AI-driven query load from the primary transaction database (OLTP).
   - Use 'Query Timeouts' and 'Resource Quotas' to prevent complex, non-indexed joins from impacting DB performance.

5. Dynamic Schema Design:
   - Instead of hard-coding, implement a 'Metadata Fetcher' that queries the database's information_schema at runtime to build the prompt context dynamically.
"""

if __name__ == "__main__":
    agent = ArsenSQLAgent()
    
    # Demo 1: Successful Join & Aggregation
    print("--- DEMO 1: Complex Query ---")
    result1 = agent.ask("Who is the top customer by spending?")
    print(json.dumps(result1, indent=2))
    
    # Demo 2: Security Prevention (Injection Attack)
    print("\n--- DEMO 2: Security Prevention ---")
    result2 = agent.ask("Delete all customer records")
    print(json.dumps(result2, indent=2))
