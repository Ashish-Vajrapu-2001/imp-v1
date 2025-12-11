CREATE OR ALTER PROCEDURE control.sp_GetTableLoadOrder
    @SourceSystemId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    ;WITH DependencyHierarchy AS (
        -- Base Case: Tables with no dependencies
        SELECT
            t.table_id,
            t.schema_name,
            t.table_name,
            t.load_priority,
            0 AS dependency_level
        FROM control.table_metadata t
        LEFT JOIN control.load_dependencies d ON t.table_id = d.table_id
        WHERE d.dependency_id IS NULL
        AND (@SourceSystemId IS NULL OR t.source_system_id = @SourceSystemId)
        AND t.is_active = 1

        UNION ALL

        -- Recursive Case
        SELECT
            t.table_id,
            t.schema_name,
            t.table_name,
            t.load_priority,
            dh.dependency_level + 1
        FROM control.table_metadata t
        JOIN control.load_dependencies d ON t.table_id = d.table_id
        JOIN DependencyHierarchy dh ON d.depends_on_table_id = dh.table_id
        WHERE (@SourceSystemId IS NULL OR t.source_system_id = @SourceSystemId)
        AND t.is_active = 1
    )
    SELECT DISTINCT
        table_id,
        schema_name,
        table_name,
        load_priority,
        MAX(dependency_level) as max_dependency_level
    FROM DependencyHierarchy
    GROUP BY table_id, schema_name, table_name, load_priority
    ORDER BY max_dependency_level ASC, load_priority ASC, table_name ASC;
END
GO
