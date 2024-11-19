import argparse
import os
import re
import pandas as pd
import numpy as np
import shutil
from logger import logger
from config import config

_LINEAGE_FOLDER = config['folders']['lineaje_folder']
_LINEAGE_FIELDS = config['folders']['parameter_file_folder']

# Validation parameters function
def validate_parameters():
    '''
        Auxiliar function to validate parameters
        Returns:
            args: input parameters
    '''
    logger.info('Validating input parameters')
    parser = argparse.ArgumentParser()
    if need_legacy():
        parser.add_argument('--legado', type=str, required=True, help='Legacy to process')
    else:
        parser.add_argument('--legado', type=str, default='dummy')
    args = parser.parse_args()

    return args


def need_legacy():
    '''
        Auxiliar function to check if legacy is needed
        Returns:
            bool: True if legacy is needed, False otherwise
    '''
    dmstask = config.getboolean('dmstask', 'active', fallback=False)
    government = config.getboolean('government', 'active', fallback=False)
    dataquality = config.getboolean('dataquality', 'active', fallback=False)

    if dmstask or government or dataquality:
        return True
    return False


# Prepare folder strcuture functions
def create_folder_structure(legacy: str):
    '''
        Function to create the folder structure for a given legacy.
        Parameters:
            legacy (str): Name of the legacy for which the folder structure will be created.
    '''
    logger.info(f"Creating folder structure for {legacy}")

    # Parent folder
    create_folder(config.get('folders', 'output_folder'))

    # Function folders
    if config.getboolean('dmstask', 'active', fallback=False):
        logger.debug(f"Creating dmstask folder structure for {legacy}")
        create_folder(config.get('folders', 'dmstask_output_folder'))
        delete_folder(f"{config.get('folders', 'dmstask_output_folder')}/{legacy}")
        create_folder(f"{config.get('folders', 'dmstask_output_folder')}/{legacy}")
    if config.getboolean('government', 'active', fallback=False):
        logger.debug(f"Creating government tables folder structure for {legacy}")
        create_folder(config.get('folders', 'government_output_folder'))
        delete_folder(f"{config.get('folders', 'government_output_folder')}/{legacy}")
        create_folder(f"{config.get('folders', 'government_output_folder')}/{legacy}")
    if config.getboolean('dataquality', 'active', fallback=False):
        logger.debug(f"Creating dataquality folder structure for {legacy}")
        create_folder(config.get('folders', 'dataquality_output_folder'))
        delete_folder(f"{config.get('folders', 'dataquality_output_folder')}/{legacy}")
        create_folder(f"{config.get('folders', 'dataquality_output_folder')}/{legacy}")
    if config.getboolean('parameters', 'active', fallback=False):
        create_folder(config.get('folders', 'parameter_output_folder'))

    logger.info(f"Folder structure created for {legacy}")


def delete_folder(folder: str):
    '''
        Auxiliar function to delete folder
        Parameters:
            folder (str): folder path
        Exceptions:
            Exception: Raised if failure deleting folder
    '''
    try:
        if os.path.exists(folder):
            shutil.rmtree(folder)
    except Exception as err:
        logger.error(f"Error deleting folder: {err}")
        raise Exception(f"Error deleting folder: {err}")


def create_folder(folder: str):
    '''
        Auxiliar function to create folder
        Parameters:
            folder (str): folder path
        Exceptions:
            Exception: Raised if failure creating folder
    '''
    try:
        if not os.path.exists(folder):
            os.mkdir(folder)
    except Exception as err:
        logger.error(f"Error creating folder: {err}")
        raise Exception(f"Error creating folder: {err}")


# Config file functions
def get_config(schema: str):
    '''
        The function reads json files which contains the informaton about all fields for each table.
        Parameters:
            schema (str): Schema name for get the fields
        Returns:
            pd.DataFrame: Dataframe with all fields for the schema
    '''
    config_df = pd.read_csv(_LINEAGE_FIELDS, sep=';', dtype=str, header=0)
    config_df['primary_key'] = config_df['primary_key'].fillna('N')
    config_df['not_null'] = config_df['not_null'].fillna('N')
    config_df['field_length'] = config_df['field_length'].fillna(0)
    config_df.ffill(inplace=True)
    config_df = config_df[config_df['schema'] == schema]
    config_df.reset_index(drop=True, inplace=True)
    config_df = config_df.map(lambda x: x.upper() if isinstance(x, str) else x)
    config_df.columns = map(str.upper, config_df.columns)

    return config_df


# Excel lineage files functions
def parse_lineage_excel(legacy: str, file_path: str, schema: str):
    '''
        Function to read the lineage file for a given legacy.
        Parameters:
            legacy (str): Name of the legacy file to read.
            file_path (str): Path to the lineage file.
            schema (str): Schema name for get the fields
        Returns:
            pd.DataFrame: DataFrame containing the lineage information.
    '''
    logger.info(f"Reading lineage excel for {legacy}")

    lineage_df = _parse_lineage_and_extract_information(file_path, schema)

    # Workaround para DET_EPISODIOS y DET_APUNTES
    lineage_df['LEGACY_NOMBRE_VISTA'] = np.where(lineage_df['LEGACY_NOMBRE_VISTA'] == f'{legacy}_VM_HSTA_DET_EPISODIO', f'{legacy}_VM_HSTA_DET_EPISODIOS', lineage_df['LEGACY_NOMBRE_VISTA'])
    lineage_df['LEGACY_NOMBRE_VISTA'] = np.where(lineage_df['LEGACY_NOMBRE_VISTA'] == f'{legacy}_VM_HSTA_DET_APUNTE', f'{legacy}_VM_HSTA_DET_APUNTES', lineage_df['LEGACY_NOMBRE_VISTA'])
    lineage_df['LEGACY_NOMBRE_VISTA'] = np.where(lineage_df['LEGACY_NOMBRE_VISTA'] == f'{legacy}_VM_HSTA_UNIDAD_FAMILIAR', f'{legacy}_VM_HSTA_UNIDADES_FAMILIARES', lineage_df['LEGACY_NOMBRE_VISTA'])

    return lineage_df


def get_last_lineage_file(legacy: str):
    '''
        Function to get the latest version of the lineage file corresponding to a legacy file.
        Parameters:
            legacy (str): Name of the legacy file for which the latest lineage file will be searched.
        Returns:
            str: The full path of the last lineage file found.
        Exceptions:
            FileNotFoundError: Raised if no lineage file is found for the specified legacy.
    '''
    logger.info(f"Getting last lineage excel for {legacy}")

    lineages = _get_lineages_legacy(legacy)

    lineage_legacy_list = []
    for item in lineages:
        if item.__contains__(legacy):
            lineage_legacy_list.append(item)

    if len(lineage_legacy_list) == 0:
        logger.info(f'No lineage found for {legacy}')
        return 

    last_lineage = _get_last_version(lineage_legacy_list)
    if last_lineage:
        logger.info(f'Got the last lineage: {last_lineage}')
        return f'{_LINEAGE_FOLDER}{last_lineage}'
    return


def _get_lineages_legacy(legacy: str):
    '''
        Function to get all lineage files for a given legacy.
        Parameters:
            legacy (str): Name of the legacy file for which lineage files will be searched.
        Returns:
            list: List of lineage file names that contain the legacy name.
    '''
    lineage_file_list = os.listdir(_LINEAGE_FOLDER)
    lineage_file_list = [item for item in lineage_file_list if item.__contains__(legacy)]

    return lineage_file_list


def _get_last_version(lineage_file_list: list):
    '''
        Function to get the latest version of a lineage file based on a version pattern.
        Parameters:
            lineage_file_list (list): List of lineage file names that contain versions in their name.
        Returns:
            str: The name of the file that has the latest version.
        Exceptions:
            FileNotFoundError: Raised if no valid version is found in the list of lineage files.
    '''
    logger.debug(f"Getting last version for {lineage_file_list}")

    # Defines
    pattern = r'v([0-9]+\.[0-9]+)'
    last_version = 0
    return_item = ''

    for item in lineage_file_list:
        version_file = re.search(pattern, item)
        if version_file is None: break
        version_file = float(version_file.group(1))
        if last_version < version_file:
            return_item = item
            last_version = version_file

    logger.debug(f'Got the last version: {last_version}')

    if return_item == '':
        logger.error(f'No version found for {lineage_file_list}')
        return

    return return_item


def _parse_lineage_and_extract_information(file_path: str, schema: str):
    '''
        This function read and excel sheet for schema and return dataframe with the needed info
        Parameters:
            file_path (str): Path to the lineage file.
            schema (str): Schema name for get the fields
        Return:
            pd.Dataframe: Dataframe with the needed info
    '''
    logger.debug(f'Checking {schema} for {file_path}')

    # Defines
    src_df = pd.DataFrame()
    lookup_column_name = {
        "ruu": "Tabla Legacy VM [FUENTE]",
        "russ": "Tabla Legacy [FUENTE]"
    }

    # Check if the sheet for RUU/RUSS exists
    excel_file = pd.ExcelFile(file_path)
    for sheet in excel_file.sheet_names:
        if sheet.lower().__contains__(schema.lower()):
            src_df = pd.read_excel(file_path, sheet_name=sheet, dtype=str, header=[0,1])
            break

    if src_df.empty:
        logger.error(f'No sheet found for {schema}')
        return pd.DataFrame()

    legacy_info_df = _get_subset_df(src_df, lookup_column_name.get(schema))
    landing_info_df = _get_subset_df(src_df, 'LANDING')
    staging_info_df = _get_subset_df(src_df, 'STAGING')
    values_df = _get_subset_df(src_df, 'Valores Formateados')

    # Transform dataframe
    all_info_df = pd.concat([legacy_info_df, values_df, landing_info_df, staging_info_df], axis=1)
    all_info_df.iloc[:, 0] = all_info_df.iloc[:, 0].ffill()
    all_info_df = all_info_df[all_info_df.iloc[:, 1].notna()].fillna('N/A').reset_index(drop=True)
    columns_to_modify = ['LEGACY_OBLIGATORIO', 'LANDING_OBLIGATORIO', 'STAGING_OBLIGATORIO']
    all_info_df[columns_to_modify] = all_info_df[columns_to_modify].apply(
        lambda col: col.apply(lambda x: True if 'S' in str(x).upper() else False)
    )

    logger.debug("Returned dataframe with excel information.")
    return all_info_df


def _get_subset_df(src_df: pd.DataFrame, columns_to_search: str):
    '''
        Function to get a subset of a dataframe based on a column and value.
        Parameters:
            df (pd.DataFrame): DataFrame to get the subset from.
            column (str): Name of the column to filter.
            value (str): Value to filter the column.
        Returns:
            pd.DataFrame: Subset of the original DataFrame.
    '''
    # Search columns with the legacy information
    lookup_columns = src_df.columns[src_df.columns.get_level_values(0) == columns_to_search]
    if not lookup_columns.empty:
        first_column_index = src_df.columns.get_loc(lookup_columns[0])
        last_column_index = first_column_index + len(lookup_columns) # eliminamos la columna de valores
    else:
        return pd.DataFrame()
    
    subset_df = src_df.iloc[:, first_column_index:last_column_index]
    if columns_to_search in ('LANDING', 'STAGING'):
        subset_df.columns = [f"{columns_to_search.upper()}_{col.upper().replace(' ', '_')}" for col in subset_df.columns.get_level_values(1)]
        subset_df = subset_df.iloc[:, :-1]
    elif columns_to_search.upper() == 'VALORES FORMATEADOS':
        subset_df.columns = [f"{col.upper().replace(' ', '_')}" for col in subset_df.columns.get_level_values(0)]
    else:
        subset_df.columns = [f"LEGACY_{col.upper().replace(' ', '_')}" for col in subset_df.columns.get_level_values(1)]
        subset_df = subset_df.iloc[:, :-1]

    return subset_df