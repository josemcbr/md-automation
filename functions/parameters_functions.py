import pandas as pd
import json
import copy
from config import config
from logger import logger


_TABLE_CONFIG_FILE = config.get('parameters','table_config_file')
_VALIDATIONS_FILE = config.get('parameters','validations_file')
_OUTPUT_FOLDER = config.get('folders', 'parameter_output_folder')


def generate_parameters(config_df: pd.DataFrame, schema: str):
    """
        This function processes unique legacy views from the given DataFrame, constructs a parameters 
        object for each legacy table, and writes the resulting data to a JSON file in the specified output folder.

        Parameters:
            config_df (pd.DataFrame): The configuration DataFrame containing table information
            schema (str): The schema name to be used 
    """
    table_config = _get_config_from_file(_TABLE_CONFIG_FILE)
    validations = _get_config_from_file(_VALIDATIONS_FILE)
    
    legacy_tables = config_df['LEGACY_VIEW'].unique().tolist()
    for legacy_table in legacy_tables:
        logger.debug(f'Generating file for {legacy_table}')

        # Filter the DataFrame for the current table
        config_df_filtered = config_df[config_df['LEGACY_VIEW'] == legacy_table].reset_index(drop=True)
        target_table = config_df_filtered['TARGET_TABLE'].unique()[0]

        specific_config = table_config.get(target_table.lower())
        parameters = _build_parameter_structure(config_df_filtered, target_table, specific_config, validations, schema)

        with open(f'{_OUTPUT_FOLDER}/{target_table.lower()}.json', 'w') as fp:
            json.dump(parameters, fp, indent=4)


def _build_parameter_structure(config_df: pd.DataFrame, table: str, table_config: dict, validations: dict, schema: str):
    """
        This function constructs a dictionary representing the parameters structure for a given table.
        Parameters:
            config_df (pd.DataFrame): The configuration DataFrame containing table information
            table (str): The name of the table for which to build the parameters structure
            table_config (dict): The configuration dictionary for the specific table
            validations (dict): The validations dictionary containing validation rules
            schema (str): The schema name to be used
        Returns:
            dict: A dictionary representing the parameters structure for the given table
    """
    params = {}
    _set_fixed_values(params)
    _set_table_params(params, table)
    _set_params_from_cfg(params, table_config)
    _set_validation_raw(params, config_df, validations, table)
    _set_source(params, schema)

    return params


def _set_fixed_values(params: dict):
    """
        This function sets fixed values in the parameters dictionary.
        Parameters:
            params (dict): The parameters dictionary to be updated
    """
    _set_s3_prefix(params)
    _set_db_name(params)
    _set_incremental_load(params)
    _set_db_name_loads(params)
    _set_mdm_params(params)


def _set_table_params(params: dict, table: str):
    """
        This function sets table-specific parameters in the parameters dictionary.
        Parameters:
            params (dict): The parameters dictionary to be updated
            table (str): The name of the table for which to set the parameters
    """
    _set_s3_target_prefix(params, table)
    _set_origin(params, table)
    _set_target_table_name(params, table)
    _set_ruleset_name(params, table)


def _set_params_from_cfg(params: dict, table_config: dict):
    """
        This function sets parameters from a configuration dictionary in the parameters dictionary.
        Parameters:
            params (dict): The parameters dictionary to be updated
            table_config (dict): The configuration dictionary containing the parameters to be set
    """
    _set_validation_accuracy(params, table_config.get('diccionario_validacion_accurracy'))
    _set_mdm_hsuid_column(params, table_config.get('mdm_hsuid_column'))
    _set_persona_usuaria_colum(params, table_config.get('persona_usuaria_column'))
    _set_orden_carga(params, table_config.get('orden_carga'))
    _set_mdm_table(params, table_config.get('mdm_table'))
    _set_mdm_tiposdatos(params, table_config.get('mdm_table_tiposdatos'))
    _set_tablas_join_mdm(params, table_config.get('tablas_join_mdm'))
    _set_order_fields(params, table_config.get('order_fields'))
    _set_dynamodb_table(params, table_config.get('dynamoDB_table'))


# Build validation field
def _set_validation_raw(params: dict, config_df: pd.DataFrame, validations_dict: dict, table: str):
    """
        This function sets the validation_raw field in the parameters dictionary.
        Parameters:
            params (dict): The parameters dictionary to be updated
            config_df (pd.DataFrame): The configuration DataFrame containing table information
            validations_dict (dict): The validations dictionary containing validation rules
            table (str): The name of the table for which to set the validation_raw field
    """
    validation_raw = {}
    _get_mapping(validation_raw, config_df)
    _get_no_nulos_vacios(validation_raw, config_df)
    _get_borrar_duplicados(validation_raw, config_df)
    _get_validations(validation_raw, config_df, validations_dict, table)
    if table.lower() == 'hsta_identificaciones':
        _get_dni_validation(validation_raw, validations_dict)
    _get_clave_primaria(validation_raw, config_df)
    _get_incrementals(validation_raw)

    params['diccionario_validacion_raw'] = json.dumps(validation_raw)


def _get_mapping(validation_raw: dict, config_df: pd.DataFrame):
    """
        This function sets the mapping field in the validation_raw dictionary.
        Parameters:
            validation_raw (dict): The validation_raw dictionary to be updated
            config_df (pd.DataFrame): The configuration DataFrame containing table information
    """
    fields = config_df['FIELD_NAME'].tolist()
    fields.append('TIMESTAMP_CARGA')
    fields_mdm = config_df['MDM_FIELD_NAME'].to_list()
    fields_mdm.append('TIMESTAMP_CARGA')
    validation_raw.update({"MAPPING": [{"ORIGIN_COLUMNS": fields},{"TARGET_COLUMNS": fields_mdm}]})


def _get_no_nulos_vacios(validation_raw: dict, config_df: pd.DataFrame):
    """
        This function sets the NO_NULOS_VACIOS field in the validation_raw dictionary.
        Parameters:
            validation_raw (dict): The validation_raw dictionary to be updated
            config_df (pd.DataFrame): The configuration DataFrame containing table information
    """
    fields = config_df[config_df['NOT_NULL'] == 'Y']['FIELD_NAME'].to_list()
    validation_raw.update({"NO_NULOS_VACIOS": fields})


def _get_borrar_duplicados(validation_raw: dict, config_df: pd.DataFrame):
    """
        This function sets the BORRAR_DUPLICADOS field in the validation_raw dictionary.
        Parameters:
            validation_raw (dict): The validation_raw dictionary to be updated
            config_df (pd.DataFrame): The configuration DataFrame containing table information
    """
    fields = config_df[config_df['PRIMARY_KEY'] == 'Y']['FIELD_NAME'].to_list()
    
    duplicate = {
        "BORRAR_DUPLICADOS": [
            {
                "FECHA_DE_MODIFICACION": config.get('parameters', 'fecha_modificacion')
            },
            {
                "PARTICIONES": fields
            }
        ]
    }
    validation_raw.update(duplicate)


def _get_validations(validation_raw: dict, config_df: pd.DataFrame, validations: dict, table: str):
    """
        This function sets the VALIDACIONES field in the validation_raw dictionary.
        Parameters:
            validation_raw (dict): The validation_raw dictionary to be updated
            config_df (pd.DataFrame): The configuration DataFrame containing table information
            validations (dict): The validations dictionary containing validation rules
            table (str): The name of the table for which to set the VALIDACIONES field
    """
    validations_copy = copy.deepcopy(validations)
    apply_validations = []
    fields = config_df['FIELD_NAME'].to_list()    
    for field in fields:
        vals = validations_copy.get(field)
        if vals:
            validation_field = _get_validations_per_field(field, vals, table)
            if validation_field:
                apply_validations.append(validation_field)
    validation_raw.update({"VALIDACIONES": apply_validations})


def _get_validations_per_field(field: str, validations: list, table: str):
    """
        This function processes validation rules for a specific field and returns a dictionary
        representing the validation rules for that field.
        Parameters:
            field (str): The name of the field for which to process validation rules
            validations (list): A list of validation rules for the field
            table (str): The name of the table for which to process validation rules
        Returns:
            dict: A dictionary representing the validation rules for the field
    """
    validation_list = []
    for val in validations:
        if _check_if_apply_validation(table.lower(), val.get('TABLAS_APLICA')):
            del val['TABLAS_APLICA']
            validation_list.append(val)
    if validation_list:
        return {field: validation_list}
    return None


def _check_if_apply_validation(table: str, tables_list: list):
    """
        This function checks if a validation rule should be applied based on the list of tables
        specified in the rule.
        Parameters:
            table (str): The name of the table for which to check if the validation rule applies
            tables_list (list): A list of table names for which the validation rule applies
        Retruns:
            bool: True if the validation rule should be applied, False otherwise
    """
    if table in tables_list:
        return True
    else:
        return False


def _get_clave_primaria(validation_raw: dict, config_df: pd.DataFrame):
    """
        This function sets the CLAVE_PRIMARIA field in the validation_raw dictionary.
        Parameters:
            validation_raw (dict): The validation_raw dictionary to be updated
            config_df (pd.DataFrame): The configuration DataFrame containing table information
    """ 
    fields = config_df[config_df['PRIMARY_KEY'] == 'Y']['FIELD_NAME'].to_list()
    fields_mdm = config_df[config_df['PRIMARY_KEY'] == 'Y']['MDM_FIELD_NAME'].to_list()
    validation_raw.update({"CLAVE_PRIMARIA": [fields, fields_mdm]})


def _get_incrementals(validation_raw: dict):
    """
        This function sets the INCREMENTALS field in the validation_raw dictionary.
        Parameters:
            validation_raw (dict): The validation_raw dictionary to be updated
    """
    incremental = {"FECHA_INCREMENTAL": config.get('parameters', 'fecha_incremental')}
    incremental_mdm = {"FECHA_INCREMENTAL": config.get('parameters', 'fecha_incremental_mdm')}
    validation_raw.update({"INCREMENTALS": [incremental, incremental_mdm]})


def _get_dni_validation(validation_raw: dict, validations: dict):
    """
        This function sets the VALIDACION_DNI field in the validation_raw dictionary.
        Parameters:
            validation_raw (dict): The validation_raw dictionary to be updated
            validations (dict): The validations dictionary containing validation rules
    """
    dni_val = validations.get('VALIDACION_DNI')
    validation_raw.update({"VALIDACION_DNI": dni_val})


# Functions to set fixed values
def _set_s3_prefix(params: dict):
    """
        This function sets the s3_prefix field in the parameters dictionary.
        Parameters:
            params (dict): The parameters dictionary to be updated
    """
    params['s3_prefix'] = config.get('parameters', 's3_prefix')


def _set_db_name(params: dict):
    """
        This function sets the dbName field in the parameters dictionary.
        Parameters:
            params (dict): The parameters dictionary to be updated
    """
    params['dbName'] = config.get('parameters', 'db_name')


def _set_incremental_load(params: dict):
    """
        This function sets the incrementalLoad field in the parameters dictionary.
        Parameters:
            params (dict): The parameters dictionary to be updated
    """
    params['cargaIncremental'] = config.get('parameters', 'carga_incremental')


def _set_s3_target_prefix(params: dict, table: str):
    """
        This function sets the s3_target_prefix field in the parameters dictionary.
        Parameters:
            params (dict): The parameters dictionary to be updated
            table (str): The name of the table for which to set the s3_target_prefix field
    """
    params['s3_target_prefix'] = table.lower()


def _set_mdm_params(params: dict):
    """
        This function sets the mdm parameters in the parameters dictionary.
        Parameters:
            params (dict): The parameters dictionary to be updated
    """
    params['mdm_dbname'] = config.get('parameters', 'mdm_dbname')
    params['mdm_uniqueid_table'] = config.get('parameters', 'mdm_uniqueid_table')
    params['mdm_uniqueid_column'] = config.get('parameters', 'mdm_uniqueid_column')
    params['mdm_originid_column'] = config.get('parameters', 'mdm_originid_column')
    params['mdm_sourceid_column'] = config.get('parameters', 'mdm_sourceid_column')


# Set params with table name
def _set_origin(params: dict, table: str):
    """
        This function sets the origin field in the parameters dictionary.
        Parameters:
            params (dict): The parameters dictionary to be updated
            table (str): The name of the table for which to set the origin field
    """
    params['origin'] = table.lower()


def _set_db_name_loads(params: dict):
    """
        This function sets the dbNameLoads field in the parameters dictionary.
        Parameters:
            params (dict): The parameters dictionary to be updated
    """
    params['dbNameLoads'] = config.get('parameters', 'db_name_cargas')


def _set_target_table_name(params: dict, table: str):
    """
        This function sets the target_table_name field in the parameters dictionary.
        Parameters:
            params (dict): The parameters dictionary to be updated
            table (str): The name of the table for which to set the target_table_name field
    """
    params['target_table_name'] = table.lower()


def _set_ruleset_name(params: dict, table: str):
    """
        This function sets the ruleset_name field in the parameters dictionary.
        Parameters:
            params (dict): The parameters dictionary to be updated
            table (str): The name of the table for which to set the ruleset_name field
    """
    params['ruleset_staging_etl'] = f'{config.get('parameters','ruleset_name_prefix')}_{table.lower()}'


# Set params from config
def _set_validation_accuracy(params: dict, value: str):
    """
        This function sets the diccionario_validacion_accurracy field in the parameters dictionary.
        Parameters:
            params (dict): The parameters dictionary to be updated
            value (str): The value to be set for the diccionario_validacion_accurracy field
    """
    if value:
        params['diccionario_validacion_accurracy'] = json.dumps(value)


def _set_mdm_hsuid_column(params: dict, value: str):
    """
        This function sets the mdm_hsuid_column field in the parameters dictionary.
        Parameters:
            params (dict): The parameters dictionary to be updated
            value (str): The value to be set for the mdm_hsuid_column field
    """
    if value:
        params['mdm_hsuid_column'] = value


def _set_persona_usuaria_colum(params: dict, value: str):
    """
        This function sets the persona_usuaria_column field in the parameters dictionary.
        Parameters:
            params (dict): The parameters dictionary to be updated
            value (str): The value to be set for the persona_usuaria_column field
    """
    if value:
        params['persona_usuaria_column'] = value


def _set_orden_carga(params: dict, value: str):
    """
        This function sets the orden_carga field in the parameters dictionary.
        Parameters:
            params (dict): The parameters dictionary to be updated
            value (str): The value to be set for the orden_carga field
    """
    if value:
        params['orden_carga'] = value


def _set_mdm_table(params: dict, value: str):
    """
        This function sets the mdm_table field in the parameters dictionary.
        Parameters:
            params (dict): The parameters dictionary to be updated
            value (str): The value to be set for the mdm_table field
    """
    if value:
        params['mdm_table'] = value


def _set_mdm_tiposdatos(params:dict, value: str):
    """
        This function sets the mdm_table_tiposdatos field in the parameters dictionary.
        Parameters:
            params (dict): The parameters dictionary to be updated
            value (str): The value to be set for the mdm_table_tiposdatos field
    """
    if value:
        params['mdm_table_tiposdatos'] = json.dumps(value)


def _set_tablas_join_mdm(params: dict, value: list):
    """
        This function sets the tablas_join_mdm field in the parameters dictionary.
        Parameters:
            params (dict): The parameters dictionary to be updated
            value (list): The list of values to be set for the tablas_join_mdm field
    """
    if value:
        params['tablas_join_mdm'] = [item.lower() for item in value]


def _set_order_fields(params: dict, value: list):
    """
        This function sets the order_fields field in the parameters dictionary.
        Parameters:
            params (dict): The parameters dictionary to be updated
            value (list): The list of values to be set for the order_fields field
    """
    if value:
        params['order_fields'] = [item.upper() for item in value]


def _set_dynamodb_table(params: dict, value: str):
    """
        This function sets the dynamoDB_table field in the parameters dictionary.
        Parameters:
            params (dict): The parameters dictionary to be updated
            value (str): The value to be set for the dynamoDB_table field
    """
    if value:
        params['dynamoDB_table'] = f"{config.get('parameters', 'dynamoDB_table_prefix')}_{config.get('parameters','environment')}"


# Another functions
def _set_source(params: dict, schema: str):
    """
        This function sets the fuente field in the parameters dictionary.
        Parameters:
            params (dict): The parameters dictionary to be updated
            schema (str): The value to be set for the fuente field
    """
    params['fuente'] = schema.upper()


# Reads for config files
def _get_config_from_file(path):
    """
        This function reads a JSON file and returns its contents as a dictionary.
        Parameters:
            path (str): The path to the JSON file to be read
        Returns:
            dict: The contents of the JSON file as a dictionary
    """
    with open(path, 'r') as f:
        data = json.load(f)
    return data
