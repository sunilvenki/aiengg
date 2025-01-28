import os
import streamlit as st
import zipfile
import subprocess
from openai import OpenAI
import time, shutil

# Set page configuration to wide mode
st.set_page_config(
    page_title="BCT - ETL Agentic AI for Data Engineering",
    page_icon="🤖",
    # layout="wide"  # Enable wide mode
)

# Initialize OpenAI client
client = OpenAI()

def code_generate(user_input):
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": """
    You are a Databricks and Python expert. Your task is to generate Python code strictly based on the user-provided project instructions. Follow the guidelines below to create clean, reusable, and structured ETL code. Do not include any explanations or extra sentences—provide only the requested code.

### **Coding Guidelines:**
1. **Project Structure:**
   - Create a project folder in the current directory named after the project title provided in the instructions.
   - Generate separate Python files (`common.py`, `bronze.py`, `silver.py`, `gold.py`) corresponding to the ETL pipeline stages.
   - Use the `# %%` delimiter to split code into logical cells in each file. Add comments in the format `### Title` for each cell.

2. **Use of Databricks SQL and PySpark:**
   - Use Databricks SQL for querying and PySpark for data transformations where applicable.
   - Write all data into Delta tables unless otherwise specified.
   - Use like df.write.saveAsTable(table_name, **options) instead of delta paths for creating tables.

3. **Function Definitions:**
   - Define reusable wrapper functions (e.g., reading data, writing to Delta tables) in `common.py`.
   - Functions must include parameters for configurable elements (e.g., URLs, table names).
   - Import and use these functions in the respective `bronze.py`, `silver.py`, and `gold.py` notebooks.

4. **Notebook Cell Structure:**
   - Each notebook must follow this structure:
     - **Cell 1:** Import necessary libraries.
     - **Cell 2:** Define reusable helper functions (if any are specific to this notebook).
     - **Cell 3:** Perform transformations and logic specific to the ETL stage.
     - **Cell 4:** Write data to the specified Delta table using the `write_to_delta` function from `common.py`.

5. **Dynamic Configurations:**
   - - Use Databricks secrets or configuration files for any URLs, table names, or paths to ensure flexibility.
   - Example: dbutils.secrets.get(scope="<SCOPE>", key="<KEY>")

6. **Error Handling and Logging:**
   - Add basic error handling for online data fetching, JSON parsing, and Spark operations.
   - Use Python’s `logging` module to capture and log critical events.

7. **Data Validation:**
   - Include validation checks for incoming data (e.g., empty data, malformed JSON, or schema mismatches).
   - Validate the schema before writing data to Delta tables.

8. **Project Initialization Code:**
   - Dynamically create the project folder and write the generated Python files into it. Use the following structure:
     ```
     notebooks = {
         "common.py": '''<common Python source code>''',
         "bronze.py": '''<bronze Python source code>''',
         "silver.py": '''<silver Python source code>''',
         "gold.py": '''<gold Python source code>''',
     }

     base_dir = "<project_name>"  # Replace with the project title
     os.makedirs(base_dir, exist_ok=True)

     for notebook_name, content in notebooks.items():
         file_path = os.path.join(base_dir, notebook_name)
         with open(file_path, "w") as f:
             f.write(content)

     base_dir
     ```

9. **Project-Specific Tasks:**  
   - Implement the tasks for each stage as per the project-specific instructions provided:
     - **common.py:** Wrapper functions for reusable tasks like reading data, writing to Delta, etc.
     - **bronze.py:** Fetch, transform, and load raw data into a bronze Delta table.
     - **silver.py:** Process the data from the bronze table, apply transformations, and load it into a silver Delta table.
     - **gold.py:** Perform analytical queries on the silver table and display or write the results.

10. **Avoid libraries:**
    - Do not use any libraries other than the standard Python libraries in code script exmaple: pySpark
    - Import os in the generating script to create the project folder and write the generated Python files into it.

11. **Final Code Output:**
    - Ensure all notebooks are well-structured, error-free, and adhere to the above guidelines.

    """
            },
            {"role": "user", "content": user_input}
        ]
    )

    return response.choices[0].message.content.strip()


# Function to display code line by line
def animate_code_in_single_block(code):
    lines = code.strip().split("\n")
    animated_code = ""  # Initialize an empty string for the code block
    code_placeholder = st.empty()  # Create a placeholder for the code block
    
    for line in lines:
        animated_code += line + "\n"  # Add the current line to the accumulated code
        code_placeholder.code(animated_code, language="python")  # Update the code block
        time.sleep(0.1)  # Delay for the animation effect

def create_zip_file(base_dir):
    zip_filename = base_dir + ".zip"
    with zipfile.ZipFile(zip_filename, 'w') as zipf:
        for root, dirs, files in os.walk(base_dir):
            for file in files:
                zipf.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), base_dir))
    return zip_filename

def display_hierarchy(zipf):
    file_tree = {}
    for file in zipf.namelist():
        parts = file.split('/')
        current_level = file_tree
        for part in parts:
            if part not in current_level:
                current_level[part] = {}
            current_level = current_level[part]
    return file_tree

def print_hierarchy(d, indent=0):
    for key, value in d.items():
        st.sidebar.write(' ' * indent + ('├── ' if indent > 0 else '') + key)
        if isinstance(value, dict):
            print_hierarchy(value, indent + 4)

st.markdown(
    "<h3 style='text-align: center;'>🤖 BCT - Agentic AI for Data Engineering</h3>", 
    unsafe_allow_html=True
)
st.sidebar.title('Options')
st.sidebar.markdown('Upload a word/text with instructions to generate Databricks code.')



# Upload file
uploaded_file = st.sidebar.file_uploader('Only .txt and .docx supported', type=['txt', 'docx'])


if uploaded_file is not None:
    content = uploaded_file.read().decode('utf-8')
    st.markdown("""
        <style>
        .stButton>button {
            background-color: white;
            color: black;
        }
        </style>
        <style>
        div.stDownloadButton > button {
            background-color: white !important;
            color: black !important;
            border: 1px solid #d1d1d1 !important;
            border-radius: 5px;
            padding: 8px 16px;
            font-size: 14px;
            font-weight: bold;
        }
        div.stDownloadButton > button:hover {
            background-color: #f0f0f0 !important;
        }
        </style>
        """, unsafe_allow_html=True)
    
    button_pressed = st.sidebar.button('Analyze and Generate Code')
    

    if button_pressed:
        with st.spinner('Analyzing...'):
            # Generate code
            code_snip = code_generate(content)
            code_snip = code_snip.replace('```python','').replace('```','')
            
            st.sidebar.success('Code skeleton ready!')
            
            animate_code_in_single_block(code_snip)

            # Write the generated code to a file
            base_dir = "generated_project"
            file_name = "generated_script.py"
            file_path = os.path.join(base_dir, file_name)

            # Remove the directory if it exists, then recreate it
            if os.path.exists(base_dir):
                shutil.rmtree(base_dir)  # Remove the directory and all its contents
            os.makedirs(base_dir)  # Recreate the directory

            with open(file_path, "w") as file:
                file.write(code_snip)
    
            

            # Change working directory to base_dir
            os.chdir(base_dir)
            
            # Execute the script and capture the output
            try:
                result = subprocess.run(
                    ["python", file_name], capture_output=True, text=True, check=True
                )
                
            except subprocess.CalledProcessError as e:
                print("Error during execution:")
                print(e.stderr)

            # Change directory to one step previous
            os.chdir("..")

            # Download generated code
            zip_filename = create_zip_file(base_dir)

            with open(zip_filename, "rb") as f:
                st.sidebar.write("Download the generated code:")
                st.sidebar.download_button('Download Code', f, file_name=zip_filename)

            st.sidebar.write("Directory Structure:") 
            # Display directory structure in zip file            
            with zipfile.ZipFile(zip_filename, 'r') as zipf:
                st.sidebar.write("generated_project.zip file contains:")
                file_tree = display_hierarchy(zipf)
                print_hierarchy(file_tree)

if uploaded_file is None:
    # Download sample file
    with open('Instructions.txt', "rb") as f:
        st.sidebar.write("Download the sample instructions file:")
        st.sidebar.download_button('Sample Instructions', f, file_name='Instructions.txt')