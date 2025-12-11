CREATE OR ALTER PROCEDURE control.sp_UpdateTableMetadata
    @TableId INT,
    @Status NVARCHAR(20),
    @PipelineRunId NVARCHAR(100),
    @RecordsLoaded INT,
    @SyncVersion BIGINT,
    @MarkInitialLoadComplete BIT = 0,
    @ErrorMessage NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Now DATETIME2 = GETDATE();

    -- Update Metadata
    UPDATE control.table_metadata
    SET
        last_load_status = @Status,
        last_load_timestamp = @Now,
        last_pipeline_run_id = @PipelineRunId,
        records_loaded = @RecordsLoaded,
        last_sync_version = CASE
            WHEN @Status = 'SUCCESS' AND @SyncVersion > 0 THEN @SyncVersion
            ELSE last_sync_version
        END,
        initial_load_completed = CASE
            WHEN @MarkInitialLoadComplete = 1 AND @Status = 'SUCCESS' THEN 1
            ELSE initial_load_completed
        END,
        modified_date = @Now
    WHERE table_id = @TableId;

    -- Log Execution
    INSERT INTO control.pipeline_execution_log (
        pipeline_run_id,
        table_id,
        execution_status,
        records_processed,
        end_time,
        error_message
    )
    VALUES (
        @PipelineRunId,
        @TableId,
        @Status,
        @RecordsLoaded,
        @Now,
        @ErrorMessage
    );

    -- Return updated record
    SELECT * FROM control.table_metadata WHERE table_id = @TableId;
END
GO
