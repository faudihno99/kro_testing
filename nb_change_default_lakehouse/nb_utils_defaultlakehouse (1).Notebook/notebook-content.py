# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# CELL ********************

# Import required packages
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# PARAMETERS CELL ********************

# Define global parameters
workspace_id_notebooks = "4d062563-29be-439d-9b01-f9adaad48a18"
new_lakehouse_name = "lh_gold_quadrature_projectcontrolling_dev"
new_lakehouse_workspace_id = "b4763317-2317-4097-928d-66ba2ec0feae"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Master function to manage lakehouses: remove all and update a new default
def manage_lakehouses(notebook_name, workspace_id, default_lakehouse, default_lakehouse_workspace=None):
    """
    Function to manage lakehouses in a notebook:
    1. Remove all existing lakehouses from the notebook definition.
    2. Optionally update the notebook with a new default lakehouse.
    3. Skip updating if the Lakehouse is already set to the desired state.

    Parameters:
    - notebook_name (str): Name of the notebook to update.
    - workspace_id (str): Workspace ID containing the notebook.
    - default_lakehouse (str): Name of the new lakehouse to set as default.
    - default_lakehouse_workspace (str, optional): Workspace ID of the new lakehouse.
    """
    print(f"Managing lakehouses for notebook: '{notebook_name}' in workspace: '{workspace_id}'\n")

    try:
        # Fetch and load the notebook definition
        nb_definition = json.loads(notebookutils.notebook.getDefinition(notebook_name, workspaceId=workspace_id))

        # Check if a default lakehouse already exists
        current_lakehouse = nb_definition.get('metadata', {}).get(
            'defaultLakehouse', {}).get('name', None)

        # Skip update if the current lakehouse matches the desired lakehouse
        if current_lakehouse == default_lakehouse:
            print(f"Default lakehouse '{default_lakehouse}' is already set. Skipping updates.")
            return

        # Remove existing lakehouses in the definition
        if 'dependencies' in nb_definition['metadata'] and 'lakehouse' in nb_definition['metadata']['dependencies']:
            nb_definition['metadata']['dependencies']['lakehouse'] = {}  # Clear lakehouse dependencies
            print("Existing lakehouses removed successfully.")
        else:
            print("No existing lakehouses found to remove.")

        # Update the lakehouse details (if provided)
        if default_lakehouse:
            notebookutils.notebook.updateDefinition(
                name=notebook_name,
                content=json.dumps(nb_definition),
                workspaceId=workspace_id,
                defaultLakehouse=default_lakehouse,
                defaultLakehouseWorkspace=default_lakehouse_workspace
            )
            print(f"Notebook updated with new default lakehouse: '{default_lakehouse}'.")
        else:
            # If no default lakehouse is provided, just update the cleared definition
            notebookutils.notebook.updateDefinition(
                name=notebook_name,
                content=json.dumps(nb_definition),
                workspaceId=workspace_id
            )
            print("Notebook updated with no default lakehouse.")

    except Exception as e:
        # Generalized error handling
        print(f"Error occurred while managing lakehouses: {e}")
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Define list of notebooks to change
notebooks = notebookutils.notebook.list(workspaceId = workspace_id_notebooks)
notebook_names = [nb['displayName'] for nb in notebooks if nb['displayName'].startswith('nb_gold') & nb["displayName"]!="nb_utils_defaultlakehouse"]
print("Notebooks to process: {notebook_names}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Iterate through each notebook to update default lakehouse
for notebook in notebook_names:
    (manage_lakehouses(notebook_name=notebook, 
                        workspace_id=workspace_id_notebooks, 
                        default_lakehouse=new_lakehouse_name, 
                        default_lakehouse_workspace=new_lakehouse_workspace_id))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
