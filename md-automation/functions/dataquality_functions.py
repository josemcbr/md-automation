import pandas as pd
from config import config
from logger import logger
from functions.generic_functions import create_folder

_OUTPUT_FOLDER = config['folders']['dataquality_output_folder']

def generate_dataquality(df: pd.DataFrame, config_df: pd.DataFrame, legacy: str, schema: str):
    """
    Generate data quality checks based on the provided dataframes and legacy system information.

    Parameters:
        df (pd.DataFrame): The main DataFrame containing data for data quality checks.
        config_df (pd.DataFrame): Configuration DataFrame with legacy view and field names.
        legacy (str): The legacy system identifier used in the naming conventions.

    The function processes the configuration and main DataFrames to produce data quality
    checks as CSV files, applying transformations and filtering based on defined rules.
    """
    logger.info('Generating data quality checks.')
    
    # Create legacy output folder
    create_folder(f'{_OUTPUT_FOLDER}/{legacy}')
    
    # Formalice config dataframe for join
    config_df['LEGACY_VIEW'] = config_df['LEGACY_VIEW'].str.replace('LEGADO', legacy.upper())

    # Join config and lineaje dataframe
    join_df = pd.merge(config_df, df, left_on=['LEGACY_VIEW', 'FIELD_NAME'],
                       right_on=['LEGACY_NOMBRE_VISTA','LEGACY_NOMBRE_CAMPO'], how='left')
    
    # Add column to process
    join_df['EXISTS'] = join_df['LEGACY_NOMBRE_CAMPO'].notna()

    legacy_tables = config_df['LEGACY_VIEW'].unique().tolist()
    for legacy_table in legacy_tables:
        logger.debug(f'Checking {legacy_table}')

        # Filter dataframe to process table by table
        join_df_filtered = join_df[join_df['LEGACY_VIEW'] == legacy_table].reset_index(drop=True)
        target_table = join_df_filtered['TARGET_TABLE'].unique()[0]
        
        # Generate rules
        rules = _generate_dataquality_rules(join_df_filtered, target_table)

        # Generate files by environment
        _generate_dataquality_files(rules, target_table, legacy)


def _generate_dataquality_rules(df: pd.DataFrame, table: str):
    """
        This function analyzes a DataFrame containing metadata about database columns
        and constructs a set of data quality validation rules represented as a string.

        Parameters:
            df (pd.DataFrame): A DataFrame containing metadata.
            table (str): The name of the database table for which to generate the rules.

        Returns:
            str: A string quality rules.
    """
    column_exists_list =  df.loc[df['EXISTS'] == True, 'FIELD_NAME'].tolist()
    is_complete_list = []
    if config.getboolean('dataquality', 'is_complete', fallback=False):
        is_complete_list = df.loc[df['EXISTS'] == False, 'FIELD_NAME'].tolist()
    column_value_list = df.dropna(subset=['VALORES_FORMATEADOS'])
    column_value_list = column_value_list[~column_value_list['VALORES_FORMATEADOS'].str.contains("N/A", na=False)][['FIELD_NAME', 'VALORES_FORMATEADOS']].values
    is_unique_list = df.loc[df['PRIMARY_KEY'] == 'Y', 'FIELD_NAME'].tolist()
    column_length_list = df[df['FIELD_LENGTH'] != 0][['FIELD_NAME', 'FIELD_LENGTH']].values

    rule_list = []

    # Add first rule
    rule_list.append(f'SchemaMatch "{config.get("dataquality","database")}.{table.lower()}"= 1.0')
    
    # Add schema match rule for the table
    rule_list.append(f'SchemaMatch "{config.get("dataquality", "database")}.{table.lower()}"= 1.0')

    # Add existence rules
    rule_list.extend([f'ColumnExists "{col}"' for col in column_exists_list])

    # Add completeness rules
    rule_list.extend([f'IsComplete "{col}"' for col in is_complete_list])

    # Add column values rules
    rule_list.extend([f'ColumnValues "{col}" in [{values}]' for col, values in column_value_list])

    # Add uniqueness rules
    rule_list.extend([f'IsUnique "{col}"' for col in is_unique_list])

    # Add length rules
    rule_list.extend([f'ColumnLength "{col}" <= {float(length)}' for col, length in column_length_list])

    # Join rules into a single string
    rules = f'Rules = [{", ".join(rule_list)}]'
    
    return rules


def _generate_dataquality_files(rules: str, table: str, legacy: str):
    """
        Generates data quality files based on the specified rules and saves them to a designated path.

        Parameters:
            rules (str): The data quality rules to be written into the files.
            table (str): The name of the database table associated with the rules.
            legacy (str): A legacy identifier used to structure the output folder path.
    """
    path = f'{_OUTPUT_FOLDER}/{legacy}/ruleset_01_stg_{table}'
    create_folder(path)

    for env in config.get('dataquality', 'environments').split(','):
        with open(f'{path}/value-{env}.txt', 'w') as f:
            f.write(rules.replace('environment', env))