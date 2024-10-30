import pandas as pd
import numpy as np
from config import config
from logger import logger

_OUTPUT_FOLDER = config['Folders']['government_output_folder']
_GOV_COLUMNS = [ 
    "owner", "table_name", "column_namedata_type", "column_namedata_type_aurora", "check_type", "type_create_lnd",
    "type_create", "char_length", "data_precisiondata_scale", "nullable", "format_data", "is_landing"
]

def generate_government_tables(df: pd.DataFrame, config_df: pd.DataFrame, legacy: str):
    """
        Generate government tables based on the provided dataframes and legacy system information.

        Parameters:
            df (pd.DataFrame): The main DataFrame containing data for table generation.
            config_df (pd.DataFrame): Configuration DataFrame with legacy view and field names.
            legacy (str): The legacy system identifier used in the naming conventions.

        The function processes the configuration and main DataFrames to produce government
        tables as CSV files, applying transformations and filtering based on defined rules.
    """
    # Replace 'LEGADO' with the specified legacy system in the LEGACY_VIEW column
    config_df['LEGACY_VIEW'] = config_df['LEGACY_VIEW'].str.replace('LEGADO', legacy.upper())
    
    # Merge the configuration DataFrame with the main DataFrame
    logger.info('Merging configuration and main dataframes')
    join_df = pd.merge(config_df, df, left_on=['LEGACY_VIEW', 'FIELD_NAME'],
                       right_on=['LEGACY_NOMBRE_VISTA','LANDING_NOMBRE_CAMPO'], how='left')
    
    logger.info('Processing legacy tables, one by one.')
    legacy_tables = config_df['LEGACY_VIEW'].unique().tolist()
    for legacy_table in legacy_tables:
        _process_legacy_table(join_df, legacy_table, legacy)


def _process_legacy_table(join_df: pd.DataFrame, legacy_table: str, legacy: str):
    """
    Process and generate government table for a specific legacy table.

    Parameters:
        join_df (pd.DataFrame): The merged DataFrame from configuration and main data.
        legacy_table (str): The name of the legacy table being processed.
        legacy (str): The legacy system identifier.
    """
    logger.debug(f'Getting information for {legacy_table}')
    join_df_filtered = join_df[join_df['LEGACY_VIEW'] == legacy_table].reset_index(drop=True)

    gov_df = pd.DataFrame(columns=_GOV_COLUMNS)

    # Creating the government DataFrame columns
    gov_df['table_name'] = join_df_filtered['TARGET_TABLE']
    gov_df['owner'] = f'DBA_{legacy.upper()}'
    gov_df['column_namedata_type'] = join_df_filtered['LANDING_NOMBRE_CAMPO'].fillna(join_df_filtered['FIELD_NAME'])
    gov_df['column_namedata_type_aurora'] = join_df_filtered['STAGING_CAMPO'].fillna(join_df_filtered['FIELD_NAME'])
    gov_df['check_type'] = join_df_filtered['CHECK_FIELD_TYPE']
    gov_df['is_landing'] = "True"
    
    logger.debug('Extract valueds for goverment table fields.')
    # Populate the government DataFrame with processed row data
    for index, row in join_df_filtered.iterrows():
        _populate_gov_dataframe(gov_df, index, row)

    # Save the government DataFrame to CSV
    target_table = join_df_filtered['TARGET_TABLE'].unique()[0]
    gov_df.to_csv(f'{_OUTPUT_FOLDER}/{legacy}/{target_table.lower()}.csv', index=False, sep=';')
    
    # Process staging records and save error CSV
    gov_df['is_stg'] = 'True'
    gov_df = _add_records(gov_df, legacy, target_table)
    gov_df.to_csv(f'{_OUTPUT_FOLDER}/{legacy}/{target_table.lower()}_error.csv', index=False, sep=';')


def _populate_gov_dataframe(gov_df: pd.DataFrame, index: int, row: pd.Series):
    """
    Populate the government DataFrame with data from a single row.

    Parameters:
        gov_df (pd.DataFrame): The government DataFrame to be populated.
        index (int): The current row index.
        row (pd.Series): The row data from the merged DataFrame.
    """
    # Process each row for LANDING and STAGING types
    landing_values = _process_row(row, 'LANDING')
    staging_values = _process_row(row, 'STAGING')

    gov_df.loc[index, 'type_create_lnd'] = landing_values[0]
    gov_df.loc[index, 'char_length'] = landing_values[1]
    gov_df.loc[index, 'data_precisiondata_scale'] = landing_values[2]
    gov_df.loc[index, 'type_create'] = staging_values[0]
    
    # Determine nullable status based on LANDING_OBLIGATORIO column
    gov_df.loc[index, 'nullable'] = 'N' if str(row['LANDING_OBLIGATORIO']) == 'True' else 'Y'


def _process_row(row: pd.Series, context: str):
    """
    Process a row of data to extract relevant information based on the context.

    Parameters:
        row (pd.Series): A single row from the DataFrame to process.
        context (str): The context to process the row for, such as 'LANDING' or 'STAGING'.

    Returns:
        tuple: A tuple containing the necessary data extracted from the row:
               - type_create for the respective context
               - char_length (if applicable)
               - data_precisiondata_scale (if applicable)
    """
    # Initialize default values
    data_type = np.nan
    char_length = np.nan
    number_length = np.nan

    # Fill NaN values with empty strings for consistent processing
    row = row.fillna('')

    # Determine the column to check based on the context
    column_to_check = 'LANDING_TIPO_DE_DATO' if context == 'LANDING' else 'STAGING_TIPO_DE_DATO'

    # Check for specific column and switch if empty
    if row[column_to_check] == '':
        column_to_check = 'CHECK_FIELD_TYPE'

    # Extract the data type from the relevant column
    type_column = row[column_to_check]
    if type_column.__contains__('VARCHAR2'):
        data_type = 'string'
        if context == 'LANDING':
            char_length = _extract_length(type_column)
    elif type_column == 'TIMESTAMP':
        data_type = 'timestamp'
    elif type_column == 'DATE':
        data_type = 'string'
    elif type_column.__contains__('NUMBER'):
        data_type = 'int'
        if context == 'LANDING' and type_column.__contains__('('):
            number_length = _extract_length(type_column)
    elif type_column.__contains__('FLOAT'):
        data_type = 'float'
        if context == 'LANDING' and type_column.__contains__('('):
            number_length = _extract_length(type_column)
    
    return data_type, char_length, number_length


def _extract_length(type_string: str):
    """
    Extract the length from a data type string, if available.

    Parameters:
        type_string (str): The data type string to extract length from.

    Returns:
        str: The extracted length, or NaN if not available.
    """
    if '(' in type_string and ')' in type_string:
        return type_string.split('(')[1].strip(')')
    return np.nan


def _add_records(df: pd.DataFrame, legacy: str, table: str):
    """
        Add necessary records to the government DataFrame.

        Parameters:
            gov_df (DataFrame): The government DataFrame to be modified.
            legacy (str): The legacy system identifier.
            target_table (str): The name of the target table.

        Returns:
            pd.DataFrame: Updated DataFrame after adding records.
    """
    logger.info('Adding records to government table')
    # Creating a record for the timestamp with specified attributes
    timestamp_record = {
        'owner': f'DBA_{legacy.upper()}', 'table_name': table, 'column_namedata_type': 'TIMESTAMP_CARGA', 'column_namedata_type_aurora': 'TIMESTAMP_CARGA',
        'check_type': 'DATE', 'type_create': 'string', 'type_create_lnd': 'string', 'char_length': np.nan, 'data_precisiondata_scale': np.nan, 
        'nullable': 'Y', 'is_landing': 'True', 'is_stg': 'True'
    }

    # Creating a record for key cross-reference in MDM
    it_cruza_mdm = {
        'owner': f'DBA_{legacy.upper()}', 'table_name': table, 'column_namedata_type': 'FK_CRUZA_MDM', 'column_namedata_type_aurora': 'FK_CRUZA_MDM',
        'check_type': 'VARCHAR2', 'type_create': 'string', 'type_create_lnd': 'string', 'char_length': np.nan, 'data_precisiondata_scale': np.nan,
        'nullable':'Y', 'is_landing':'True', 'is_stg':'False'
    }

    # Creating a record for foreign key incompleteness
    cd_fk_incumple = {
        'owner': f'DBA_{legacy.upper()}', 'table_name': table, 'column_namedata_type': 'CD_FK_INCUMPLE', 'column_namedata_type_aurora': 'CD_FK_INCUMPLE',
        'check_type': 'VARCHAR2', 'type_create': 'string', 'type_create_lnd': 'string', 'char_length': np.nan, 'data_precisiondata_scale': np.nan,
        'nullable':'Y', 'is_landing':'True', 'is_stg':'False'
    }

    # Creating a record for error
    registro_error = {
        'owner': f'DBA_{legacy.upper()}', 'table_name': table, 'column_namedata_type': 'ERROR', 'column_namedata_type_aurora': 'ERROR',
        'check_type': 'VARCHAR2', 'type_create': 'string', 'type_create_lnd': 'string', 'char_length': np.nan, 'data_precisiondata_scale': np.nan,
        'nullable':'Y', 'is_landing':'True', 'is_stg':'True'
    }

    # Creating a record for data quality rule
    dataquality_rule = {
        'owner': f'DBA_{legacy.upper()}', 'table_name': table, 'column_namedata_type': 'DataQualityRulesSkip', 'column_namedata_type_aurora': 'DataQualityRulesSkip',
        'check_type': 'VARCHAR2', 'type_create': 'string', 'type_create_lnd': 'string', 'char_length': np.nan, 'data_precisiondata_scale': np.nan,
        'nullable':'Y', 'is_landing':'True', 'is_stg':'False'
    }
    
    # Collecting all individual records into a list and add to df
    new_records = [timestamp_record, it_cruza_mdm, cd_fk_incumple, registro_error, dataquality_rule]
    new_df = pd.DataFrame(new_records)
    df = pd.concat([df, new_df], ignore_index=True)

    return df