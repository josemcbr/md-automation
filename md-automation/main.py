import os
import copy
import pandas as pd
from logger import logger
from functions.dmstasks_functions import generate_dmstask
from functions.government_tables_functions import generate_government_tables
from functions.generic_functions import (
    validate_parameters,
    create_folder_structure,
    get_last_lineage_file,
    parse_lineage_excel,
    get_config
)

# set directory to script path    
os.chdir(os.path.dirname(os.path.abspath(__file__)))

def main():
    logger.info('Starting process.')
    args = validate_parameters()
    create_folder_structure(args.legado)

    try:
        lineage_excel_path = get_last_lineage_file(args.legado)
        for schema in ['ruu', 'russ']:
            process_schema(schema, args, lineage_excel_path)
    except Exception as err:
        logger.error(f'Error: {err}')


def process_schema(schema, args, lineage_excel_path):
    """
        Process the given schema to extract configuration and lineage data, and generate government tables.

        Parameters:
            schema (str): The name of the schema to be processed.
            args (Namespace): Command line arguments that include legacy information.
            lineage_excel_path (str): The file path to the lineage Excel file.

        This function retrieves configuration data for the schema, parses the lineage Excel to 
        obtain lineage information, and checks for data availability. 
        If lineage data is present, it generates government tables using the relevant lineage and 
        configuration data.
    """
    logger.info(f'Processing schema {schema}')
    config_df = get_config(schema)
    lineage_df = parse_lineage_excel(args.legado, lineage_excel_path, schema)

    if lineage_df.empty:
        logger.info(f'No lineage found for {args.legado} in {schema}')
        return

    # Dmstask files
    logger.info(f'Generating dmstask files for {args.legado} in {schema}')
    generate_dmstask(copy.deepcopy(lineage_df).iloc[:, 0:4], config_df, args.legado, schema)

    # Generate government tables files
    logger.info(f'Generating government tables files for {args.legado} in {schema}')
    govement_df = copy.deepcopy(lineage_df)
    govement_df = pd.concat([govement_df.iloc[:, 0], govement_df.iloc[:, 4:]], axis=1)
    generate_government_tables(govement_df, config_df, args.legado)  
    

if __name__ == '__main__':
    main()