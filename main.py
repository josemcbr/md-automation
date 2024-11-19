import os
import copy
import pandas as pd
from logger import logger
from config import config
from functions.dmstasks_functions import generate_dmstask
from functions.government_tables_functions import generate_government_tables
from functions.dataquality_functions import generate_dataquality
from functions.parameters_functions import generate_parameters
from functions.generic_functions import (
    validate_parameters,
    need_legacy,
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

    try:
        # Create folder extructure
        create_folder_structure(args.legado)
        
        # Get excel path
        lineage_excel_path = get_last_lineage_file(args.legado)
        for schema in ['ruu', 'russ']:
            process_schema(schema, args.legado, lineage_excel_path)
    except Exception as err:
        logger.error(f'Error: {err}')
    
    logger.info('Process finished.')


def process_schema(schema: str, legacy: str, lineage_excel_path: str):
    """
        Process the given schema to extract configuration and lineage data, and generate government tables.

        Parameters:
            schema (str): The name of the schema to be processed.
            legacy (str): The name of the legacy data to be processed.
            lineage_excel_path (str): The path to the lineage Excel file.

        This function retrieves configuration data for the schema, parses the lineage Excel to 
        obtain lineage information, and checks for data availability. 
        If lineage data is present, it generates government tables using the relevant lineage and 
        configuration data.
    """
    logger.info(f'Processing schema {schema}')
    config_df = get_config(schema)

    if need_legacy() and lineage_excel_path is not None:
        lineage_df = parse_lineage_excel(legacy, lineage_excel_path, schema)
        if lineage_df.empty:
            logger.info(f'No lineage found for {legacy} in {schema}')
            raise FileNotFoundError(f'No lineage found for {legacy}')
    elif not need_legacy():
        pass
    else:
        raise FileNotFoundError(f'No lineage found for {legacy}')
   
    # Dmstask files
    if config.getboolean('dmstask', 'active', fallback=False):
        logger.info(f'Generating dmstask files for {legacy} in {schema}')
        generate_dmstask(copy.deepcopy(lineage_df).iloc[:, 0:4], config_df, legacy, schema)

    # Generate government tables files
    if config.getboolean('government', 'active', fallback=False):
        logger.info(f'Generating government tables files for {legacy} in {schema}')
        govement_df = copy.deepcopy(lineage_df)
        govement_df = pd.concat([govement_df.iloc[:, 0], govement_df.iloc[:, 5:]], axis=1)
        generate_government_tables(govement_df, config_df, legacy)  

    # DataQuality files
    if config.getboolean('dataquality', 'active', fallback=False):
        logger.info(f'Generating DataQuality files for {legacy} in {schema}')
        dataquality_df = copy.deepcopy(lineage_df)
        dataquality_df = pd.concat([dataquality_df.iloc[:, 0:2], dataquality_df.iloc[:, 4]], axis=1)
        generate_dataquality(dataquality_df, config_df, legacy, schema)

    if config.getboolean('parameters', 'active', fallback=False):
        logger.info(f'Generating Parameters files in {schema}')
        generate_parameters(config_df, schema)


if __name__ == '__main__':
    main()