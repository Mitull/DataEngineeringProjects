create SCHEMA gold

create VIEW gold.final
as 
SELECT  *
FROM OPENROWSET(
    BULK 'https://olistdatastorageaccmk.blob.core.windows.net/olistdata/Silver',
    FORMAT = 'PARQUET'
) AS result1;

SELECT * from gold.final

create VIEW gold.final2
as 
SELECT  *
FROM OPENROWSET(
    BULK 'https://olistdatastorageaccmk.blob.core.windows.net/olistdata/Silver',
    FORMAT = 'PARQUET'
) AS resul2
where order_status='delivered';


SELECT * from gold.final2

----------------------- Gold layer data 

-- CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Mitul@1998';

-- CREATE DATABASE SCOPED CREDENTIAL [WorkspaceIdentity] WITH IDENTITY = 'Managed Identity';

-- select * from sys.database_credentials


CREATE EXTERNAL FILE FORMAT extfileformat WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);
GO

CREATE EXTERNAL DATA SOURCE Goldlayer WITH (
    LOCATION = 'https://olistdatastorageaccmk.dfs.core.windows.net/olistdata/Gold/',
    CREDENTIAL = WorkspaceIdentity
);
GO

CREATE EXTERNAL TABLE gold.finaltable WITH (
        LOCATION = 'Serving',
        DATA_SOURCE = Goldlayer,
        FILE_FORMAT = extfileformat
) AS
SELECT * FROM gold.final2;
GO
select * from gold.finaltable
