CREATE OR ALTER PROCEDURE control.sp_GetCDCChanges
    @SchemaName NVARCHAR(50),
    @TableName NVARCHAR(100),
    @PrimaryKeyColumns NVARCHAR(255),
    @LastSyncVersion BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @CurrentVersion BIGINT = CHANGE_TRACKING_CURRENT_VERSION();
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @JoinCondition NVARCHAR(MAX) = '';

    -- Build Join Condition from comma-separated PKs
    SELECT @JoinCondition = STRING_AGG(
        CAST('CT.' + value + ' = T.' + value AS NVARCHAR(MAX)),
        ' AND '
    )
    FROM STRING_SPLIT(@PrimaryKeyColumns, ',');

    -- Dynamic SQL Construction
    SET @SQL = '
    SELECT
        CT.SYS_CHANGE_OPERATION,
        CT.SYS_CHANGE_VERSION,
        ' + CAST(@CurrentVersion AS NVARCHAR(20)) + ' AS _current_sync_version,
        T.*
    FROM CHANGETABLE(CHANGES ' + @SchemaName + '.' + @TableName + ', ' + CAST(@LastSyncVersion AS NVARCHAR(20)) + ') AS CT
    LEFT JOIN ' + @SchemaName + '.' + @TableName + ' AS T
        ON ' + @JoinCondition + ';
    ';

    EXEC sp_executesql @SQL;
END
GO
