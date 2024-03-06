


DEMO_DATA_FOLDER_NAME = 'DemoData'
SOURCE_FOLDER_FILE_API_PATH = f'/lakehouse/default/Files/{DEMO_DATA_FOLDER_NAME}'
SOURCE_FOLDER_ABFSS_PATH = f'abfss://c7b8b97a-3b10-4031-8b76-717a2a16065a@onelake.dfs.fabric.microsoft.com/9324fdce-fd92-49a2-ba25-3275ef268375/Files/{DEMO_DATA_FOLDER_NAME}'
TARGET_LAKEHOUSE_NAME = 'SDS_SGDE_POC_Unilock_ProcessedSocialAndGovernanceData_LH'



import os

spark.conf.set("spark.sql.caseSensitive", "true")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")



for table in os.listdir(SOURCE_FOLDER_FILE_API_PATH):
    tableFolderABFSSPath = os.path.join(SOURCE_FOLDER_ABFSS_PATH, table)
    df = spark.read.format("delta").load(tableFolderABFSSPath)
    df.write.format("delta").mode('overwrite').saveAsTable(TARGET_LAKEHOUSE_NAME + '.' + table)


CONFIG_DATALAKE_PATH = "abfss://c7b8b97a-3b10-4031-8b76-717a2a16065a@onelake.dfs.fabric.microsoft.com/9324fdce-fd92-49a2-ba25-3275ef268375"
ESG_LAKE_SCHEMA_PATH = f"{CONFIG_DATALAKE_PATH}/Files/ESGSchema.json"
TARGET_LAKEHOUSE_NAME = "SDS_SGDE_POC_Unilock_ProcessedSocialAndGovernanceData_LH"
SOCIAL_AND_GOVERNANCE_BUSINESS_AREA = "Environmental Social and Govenance"
SELECTIVE_SOCIAL_AND_GOVERNANCE_TABLES = ['BusinessEthicsMetricType', 'BusinessMetric', 'BusinessMetricType', 'Country', 'CountrySubdivisionCategory', 'DataPrivacyMetricType', 'Employee', 'EmployeeLocation', 'EmployeePartyRelationshipType', 'EmployeeRelatedParty', 'Gender', 'GeographicArea', 'HealthSafetyTrainingMetricType', 'Incident', 'IncidentCost', 'IncidentCostType', 'IncidentType', 'LearningEvent', 'LearningEventClassification', 'LearningEventPartyRelationshipType', 'LearningEventRelatedParty', 'LearningEventType', 'Location', 'MetricPurpose', 'Party', 'PartyBusinessMetric', 'PartyCustomerGroupServiceDisruption', 'PartyDataPrivacyMetric', 'PartyEvent', 'PartyGeographicAreaBusinessEthicsMetric', 'PartyGeographicAreaHealthSafetyTrainingMetric', 'PartyResponsibleSourcing', 'RacialCategory', 'ResponsibleSourcingType', 'ServiceDisruptionType', 'Standard', 'StandardTraining', 'StandardType', 'StorageContainerProductActualStorage', 'UnitOfMeasure']

import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import IntegerType, StringType, BooleanType, DateType, TimestampType, BinaryType, DecimalType, DoubleType, LongType

spark.conf.set("spark.sql.caseSensitive", "true")

# Read file from filePath
def ReadFile(filePath):
    rdd = spark.sparkContext.wholeTextFiles(filePath)
    return rdd.collect()[0][1]

# Define a mapping of data types
data_type_mapping = {
    'integer': IntegerType(),
    'string': StringType(),
    'boolean': BooleanType(),
    'date': DateType(),
    'timestamp': TimestampType(),
    'binary': BinaryType(),
    'decimal': DecimalType(18, 2),
    'double': DoubleType(),
    'long': LongType()
}

# Load schema from Model JSON file
def LoadSchemaFromModelJson(table_info):
    # Extract columns information
    columns = table_info['storageDescriptor']['columns']

    # Define the schema for the DataFrame
    fields = []

    # Iterate through the columns and create DataFrame schema fields
    for column in columns:
        name = column['name']
        datatype = column['originDataTypeName']['typeName']
        is_nullable = column['originDataTypeName']['isNullable']

        # Define additional attributes for 'date' and 'timestamp' data types
        additional_attributes = {}
        if datatype == 'date':
            # Extract 'dateFormat' for 'date' data type
            additional_attributes['dateFormat'] = column['originDataTypeName']['properties'].get('dateFormat')
        elif datatype == 'timestamp':
            # Extract 'timestampFormat' for 'timestamp' data type
            additional_attributes['timestampFormat'] = column['originDataTypeName']['properties'].get('timestampFormat')
        elif datatype == 'string':
            length = column['originDataTypeName'].get('length')
            # Set a default length of 256 for 'string' data type if length is not defined
            if length is None:
                length = 256
            additional_attributes['length'] = length
        elif datatype == 'decimal':
            # Extract 'precision,' and 'scale' for 'decimal' data type
            precision = column['originDataTypeName'].get('precision')
            scale = column['originDataTypeName'].get('scale')
            if precision is not None and scale is not None:
                data_type_mapping[datatype] = DecimalType(precision, scale)

        # Append the column information to the schema
        if datatype in data_type_mapping:
            fields.append(StructField(name, data_type_mapping[datatype], is_nullable, additional_attributes))

    # Create the DataFrame schema
    schema = StructType(fields)
    return schema

esgSchema = json.loads(ReadFile(ESG_LAKE_SCHEMA_PATH))
targetTables = spark.catalog.listTables(TARGET_LAKEHOUSE_NAME)

# Iterate through each table in metadata and create tables as required
for table_info in esgSchema:
    if SOCIAL_AND_GOVERNANCE_BUSINESS_AREA in table_info['properties']['fromBusinessAreas'].split(','):
        tableName = table_info['name']
        if (tableName not in targetTables) and (len(SELECTIVE_SOCIAL_AND_GOVERNANCE_TABLES) == 0 or tableName in SELECTIVE_SOCIAL_AND_GOVERNANCE_TABLES):
            schema = LoadSchemaFromModelJson(table_info)    
            df = spark.createDataFrame([], schema)
            df.write\
                .format("delta")\
                .mode("ignore")\
                .saveAsTable(tableName)
