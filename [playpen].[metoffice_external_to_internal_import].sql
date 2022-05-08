
/****** Object:  StoredProcedure [playpen].[p_url_splitting]    Script Date: 02/04/2022 19:05:14 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

alter PROC [playpen].[metoffice_external_to_internal_import] AS
-- exec [playpen].[metoffice_external_to_internal_import]

--------------------------------------------------------------------------------------------------------------------------------------
-- Start of Procedure
--------------------------------------------------------------------------------------------------------------------------------------

BEGIN
BEGIN TRY

SET NOCOUNT ON;



--------------------------------------------------------------------------------------------------------------------------------------
-- Log Start
--------------------------------------------------------------------------------------------------------------------------------------

DECLARE	@Rec_Count         BigInt;

DECLARE @Batch_No_PV       Uniqueidentifier,	--PV to indicate passing value
        @Proc_Name_PV      VarChar(100),
        @Proc_Call_PV      VarChar(1000),
        @Error_Detail_PV   VarChar(8000),
        @Step_Name_PV      VarChar(500);

SELECT @Batch_No_PV    = NEWID(),
       @Proc_Name_PV   = '[playpen].[metoffice_external_to_internal_import]',
       @Proc_Call_PV   = 'EXEC [playpen].[metoffice_external_to_internal_import]';
	   
EXECUTE MONITOR.P_JOB_LOG @Batch_No       = @Batch_No_PV,		
                          @Proc_Name      = @Proc_Name_PV,		
                          @Proc_Call      = @Proc_Call_PV,		
                          @Exe_Status     = 'STARTED',
                          @Error_Detail   = NULL;


--------------------------------------------------------------------------------------------------------------------------------------
-- Step 1 - Set up external table for met office data
--------------------------------------------------------------------------------------------------------------------------------------

EXECUTE MONITOR.P_JOB_LOG_STEP @Batch_No     = @Batch_No_PV,	
                               @Step_Name    = 'Step 1 - Set up external table for met office data',
                               @Step_Key     = NULL,
                               @Step_Value   = NULL;


--drop external table playpen.jm_metoffice_ext
--CREATE EXTERNAL TABLE playpen.jm_metoffice_ext
--(
--	cityName varchar(100)
--	,ForecastDay varchar(100)
--	,midday10MWindSpeed float
--	,middayVisibility float
--	,middayRelativeHumidity float
--	,dayMaxFeelsLikeTemp float
--	,dayMinFeelsLikeTemp float
--	,dayProbabilityOfPrecipitation float
--	,dayProbabilityOfSnow float
--	,dayProbabilityOfHeavySnow float
--	,dayProbabilityOfRain float
--	,dayProbabilityOfHeavyRain float
--)
--WITH (DATA_SOURCE = accurankerIngest,LOCATION = N'MetOffice',FILE_FORMAT = [myradiotimesonboardingmetadata],REJECT_TYPE = VALUE,REJECT_VALUE = 1)

--select top 10 * from playpen.jm_metoffice_ext

declare @startcount as int
select @startcount = count(*) from playpen.jm_met_office_forecast

--------------------------------------------------------------------------------------------------------------------------------------
-- Step 1.5 - Table start count
--------------------------------------------------------------------------------------------------------------------------------------

EXECUTE MONITOR.P_JOB_LOG_STEP @Batch_No     = @Batch_No_PV,	
                               @Step_Name    = 'Step 1.5 - Table start count',
                               @Step_Key     = NULL,
                               @Step_Value   = @startcount;


--------------------------------------------------------------------------------------------------------------------------------------
-- Step 2 - Delete any dates already in the table
--------------------------------------------------------------------------------------------------------------------------------------

EXECUTE MONITOR.P_JOB_LOG_STEP @Batch_No     = @Batch_No_PV,	
                               @Step_Name    = 'Step 2 - Delete any dates already in the table',
                               @Step_Key     = NULL,
                               @Step_Value   = NULL;


delete from playpen.jm_met_office_forecast
where ForecastDay in(select distinct convert(date, left(ForecastDay, 10)) from playpen.jm_metoffice_ext)


declare @endcount as int
select @endcount = count(*) from playpen.jm_met_office_forecast

declare @amountDeleted as int
set @amountDeleted = @startcount - @endcount

--------------------------------------------------------------------------------------------------------------------------------------
-- Step 2.5 - Amount deleted out of table
--------------------------------------------------------------------------------------------------------------------------------------

EXECUTE MONITOR.P_JOB_LOG_STEP @Batch_No     = @Batch_No_PV,	
                               @Step_Name    = 'Step 2.5 - Amount deleted out of table',
                               @Step_Key     = NULL,
                               @Step_Value   = @amountDeleted;


--------------------------------------------------------------------------------------------------------------------------------------
-- Step 3 - Add new data into the sql table
--------------------------------------------------------------------------------------------------------------------------------------

EXECUTE MONITOR.P_JOB_LOG_STEP @Batch_No     = @Batch_No_PV,	
                               @Step_Name    = 'Step 3 - Add new data into the sql table',
                               @Step_Key     = NULL,
                               @Step_Value   = NULL;


insert into playpen.jm_met_office_forecast
select 
	cityName
	,convert(date, left(ForecastDay, 10)) as ForecastDay
	,midday10MWindSpeed
	,middayVisibility
	,middayRelativeHumidity
	,dayMaxFeelsLikeTemp
	,dayMinFeelsLikeTemp
	,dayProbabilityOfPrecipitation
	,dayProbabilityOfSnow
	,dayProbabilityOfHeavySnow
	,dayProbabilityOfRain
	,dayProbabilityOfHeavyRain
from 
	playpen.jm_metoffice_ext


declare @finalcount as int
select @finalcount = count(*) from playpen.jm_met_office_forecast

--------------------------------------------------------------------------------------------------------------------------------------
-- Step 3.5 - Final table count
--------------------------------------------------------------------------------------------------------------------------------------

EXECUTE MONITOR.P_JOB_LOG_STEP @Batch_No     = @Batch_No_PV,	
                               @Step_Name    = 'Step 3.5 - Final table count',
                               @Step_Key     = NULL,
                               @Step_Value   = @finalcount;


--------------------------------------------------------------------------------------------------------------------------------------
-- Log Complete
--------------------------------------------------------------------------------------------------------------------------------------

EXECUTE MONITOR.P_JOB_LOG @Batch_No       = @Batch_No_PV,		
                          @Proc_Name      = @Proc_Name_PV,		
                          @Proc_Call      = @Proc_Call_PV,		
                          @Exe_Status     = 'COMPLETED',
                          @Error_Detail   = NULL;

END TRY

--------------------------------------------------------------------------------------------------------------------------------------
-- Capture Errors
--------------------------------------------------------------------------------------------------------------------------------------

BEGIN CATCH

SELECT @Error_Detail_PV = ERROR_MESSAGE();

EXECUTE MONITOR.P_JOB_LOG @Batch_No       = @Batch_No_PV,		
                          @Proc_Name      = @Proc_Name_PV,		
                          @Proc_Call      = @Proc_Call_PV,		
                          @Exe_Status     = 'ERROR',
                          @Error_Detail   = @Error_Detail_PV;

THROW

END CATCH
END

--------------------------------------------------------------------------------------------------------------------------------------
-- End of Procedure
--------------------------------------------------------------------------------------------------------------------------------------

GO


