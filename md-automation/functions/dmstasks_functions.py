import json
import pandas as pd
import copy
from logger import logger
from config import config


_OUTPUT_FOLDER = config['Folders']['dmstask_output_folder']
_GENERIC_RULE = {
    "rule-type": "transformation",
    "rule-id": "value",
    "rule-name": "value",
    "rule-target": "column",
    "object-locator": {
        "schema-name": "schema_name",
        "table-name": "table_name",
        "column-name": "column_name"
    },
    "rule-action": "action",
    "value": None,
    "old-value": None,
    "expression":"",
    "data-type":{
        "type":"type",
        "length": 1,
        "precision": 1,
        "scale": 1
    }
}


def generate_dmstask(df: pd.DataFrame, config_df: pd.DataFrame, legacy: str, schema: str):
    '''
        Function to generate and save dmstask json file for table.
        Parameters:
            df (pd.DataFrame): DataFrame with the lineage information
            config_df (pd.DataFrame): DataFrame with the configuration information
            legacy (str): Legacy name
            schema (str): Schema name
    '''
    # Recorrer el dataframe config y compararlo con el df del linaje
    legacy_tables = config_df['LEGACY_VIEW'].unique().tolist()
    for legacy_table in legacy_tables:
        logger.debug(f'Checking {legacy_table}')

        config_df_filtered = config_df[config_df['LEGACY_VIEW'] == legacy_table].reset_index(drop=True)
        df_filtered = df[df['LEGACY_NOMBRE_VISTA'] == legacy_table.replace('LEGADO',legacy.upper())].reset_index(drop=True)

        target_table = config_df_filtered['TARGET_TABLE'].unique()[0]

        config_df_filtered['PRESENT_IN_LINEAGE'] = config_df_filtered['FIELD_NAME'].isin(df_filtered['LEGACY_NOMBRE_CAMPO'])
    
        rules = _generate_file(config_df_filtered)

        with open(f'{_OUTPUT_FOLDER}/{legacy.lower()}/{target_table.lower()}_{legacy.lower()}.json', 'w') as fp:
            json.dump(rules, fp, indent=4)


def _generate_file(df: pd.DataFrame):
    '''
        Function to generate the dmstask rules.
        Parameters:
            df (pd.DataFrame): DataFrame with the configuration information fields
        Returns:
            list: List of rules for dmstask
    '''
    logger.info(f'Generating dmstask for {df["LEGACY_VIEW"].unique()[0]}')
    # Defines
    rule_list = []

    # needed vars
    legacy_table = df['LEGACY_VIEW'].unique()[0]
    target_table = df['TARGET_TABLE'].unique()[0]

    # Construct dmstask
    rule_list.append(_get_schema_rule())
    rule_list.append(_get_table_rule(legacy_table, target_table.lower()))

    counter_rule = 2
    for index, row in df.iterrows():
        dict_row = row.to_dict()
        rule_list.append(_get_field_rule(dict_row, legacy_table, counter_rule))
        counter_rule += 1

    rule_list.append(_get_timestamp_carga_rule(legacy_table, counter_rule))
    counter_rule += 1
    rule_list.append(_get_filter_rule(legacy_table, counter_rule))

    dict_rules = {"rules": rule_list}

    return dict_rules


# Function to generate rules
def _get_schema_rule():
    '''
        Function to create a schema rule for dmstask.
        Returns:
            dict: Schema rule for dmstask
    '''
    rule = copy.deepcopy(_GENERIC_RULE)

    # modify values
    rule["rule-id"] = "0"
    rule["rule-name"] = "0"
    rule["rule-target"] = "schema"
    rule["object-locator"]["schema-name"] = "nombre_schema"
    rule["rule-action"] = "rename"
    rule["value"] = "carnet"

    # delete non used keys
    del rule["object-locator"]["table-name"]
    del rule["object-locator"]["column-name"]
    del rule["expression"]
    del rule["data-type"]

    return rule


def _get_table_rule(legacy_table: str, target_table: str):
    '''
        Function to create a table rule for dmstask.
        Parameters:
            legacy_table (str): Legacy table name
            target_table (str): Target table name
        Returns:
            dict: Schema rule for dmstask
    '''
    rule = copy.deepcopy(_GENERIC_RULE)

    # modify values
    rule["rule-id"] = "1"
    rule["rule-name"] = "1"
    rule["rule-target"] = "table"
    rule["object-locator"]["schema-name"] = "nombre_schema"
    rule["object-locator"]["table-name"] = legacy_table
    rule["rule-action"] = "rename"
    rule["value"] = target_table

    # delete non used keys
    del rule["object-locator"]["column-name"]
    del rule["expression"]
    del rule["data-type"]

    return rule


def _get_field_rule(row: dict, legacy_table: str, index: int):
    '''
        Function to create field rules.
        Parameters:
            row (dict): Dictionary with the field information
            legacy_table (str): Legacy table name
            index (int): Index for the rule
        Return:
            dict: Field rule for dmstask
    '''
    rule = copy.deepcopy(_GENERIC_RULE)

    # modify values
    rule["rule-id"] = str(index)
    rule["rule-name"] = str(index)
    rule["rule-target"] = "column"
    rule["object-locator"]["schema-name"] = "nombre_schema"
    rule["object-locator"]["table-name"] = legacy_table

    if row.get('PRESENT_IN_LINEAGE'):
        rule["object-locator"]["column-name"] = row.get('FIELD_NAME')
        rule["rule-action"] = "include-column"
        del rule["expression"]
        del rule["data-type"]
    else:
        rule["rule-action"] = "add-column"
        rule["value"] = row.get('FIELD_NAME')
        del rule["object-locator"]["column-name"]
        del rule["old-value"]
        if row.get('FIELD_TYPE') == 'STRING':
            rule["data-type"]["type"] = row.get('FIELD_TYPE').lower()
            rule["data-type"]["length"] = int(row.get('FIELD_LENGTH'))
            del rule["data-type"]['precision']
            del rule["data-type"]['scale']
        elif row.get('FIELD_TYPE') == 'NUMERIC':
            rule["data-type"]["type"] = row.get('FIELD_TYPE').lower()
            rule["data-type"]["precision"] = _extract_precission_scale(row.get('FIELD_LENGTH'), 0)
            rule["data-type"]["scale"] = _extract_precission_scale(row.get('FIELD_LENGTH'), 1)
            del rule["data-type"]['length']
        else: # row.get('DATA_TYPE') == 'TIMESTAMP':
            rule["data-type"]["type"] = 'string'
            rule["data-type"]["length"] = 14
            del rule["data-type"]['precision']
            del rule["data-type"]['scale']

    return rule


def _get_timestamp_carga_rule(legacy_table: str, index: int):
    '''
        Function to create timetamp_carga rule
        Parameters:
            legacy_table (str): Legacy table name
            index (int): Index for the rule
        Return:
            dict: Field rule for dmstask
    '''
    rule = copy.deepcopy(_GENERIC_RULE)

    # modify values
    rule["rule-id"] = str(index)
    rule["rule-name"] = str(index)
    rule["rule-target"] = "column"
    rule["object-locator"]["schema-name"] = "nombre_schema"
    rule["object-locator"]["table-name"] = legacy_table
    rule["rule-action"] = "add-column"
    rule["value"] = "TIMESTAMP_CARGA"
    rule["expression"] = "{{timestamp_carga}}"
    rule["data-type"]["type"] = "string"
    rule["data-type"]["length"] = 14

    # delete non used keys
    del rule["object-locator"]["column-name"]
    del rule["old-value"]
    del rule["data-type"]["precision"]
    del rule["data-type"]["scale"]

    return rule


def _get_filter_rule(legacy_table: str, index: int):
    '''
        Function to create rule filter
        Parameters:
            legacy_table (str): Legacy table name
            index (int): Index for the rule
        Return:
            dict: Field rule for dmstask
    '''
    rule = copy.deepcopy(_GENERIC_RULE)

    # modify values
    rule["rule-type"] = "selection"
    rule["rule-id"] = str(index)
    rule["rule-name"] = str(index)
    rule["object-locator"]["schema-name"] = "nombre_schema"
    rule["object-locator"]["table-name"] = legacy_table
    rule["rule-action"] = "include"
    rule["filters"] = [
        {
            "filter-type": "source",
            "column-name": "FC_ULTIMA_ACT",
            "filter-conditions": [
                {
                    "filter-operator": "gte",
                    "value": "{{lastExecution}}"
                }
            ]
        }
    ]

    # delete non used keys
    del rule["object-locator"]["column-name"]
    del rule["value"]
    del rule["expression"]
    del rule["data-type"]
    del rule["old-value"]
    del rule["rule-target"]

    return rule


def _extract_precission_scale(length_field: str, index: int):
    '''
        Extracts the precision or scale from a comma-separated string based on the specified index.
        Parameters:
            length_field (str): A string containing information about precision and scale, separated by commas.
            index (int): The index of the precision or scale value to extract from the length_field.
        Returns:
            int: The extracted precision or scale value as an integer. If the extraction fails or
                the index is out of range, returns 0.
    '''
    try:
        return int(length_field.split(',')[index])
    except:
        return 0